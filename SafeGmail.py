import imaplib
import email
from email.header import decode_header
from email.utils import parsedate_to_datetime
import asyncio
from telegram import Bot, Update
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes, CallbackQueryHandler
from telegram import InlineKeyboardButton, InlineKeyboardMarkup
import logging
import re
from bs4 import BeautifulSoup
import psycopg2
from datetime import datetime, timedelta
import signal
import sys
import os
import urllib.parse as urlparse

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)

# Reduce spam from other loggers
logging.getLogger('httpx').setLevel(logging.WARNING)
logging.getLogger('telegram').setLevel(logging.WARNING)

# Get environment variables
TELEGRAM_TOKEN = os.getenv('TELEGRAM_TOKEN')
DATABASE_URL = os.getenv('DATABASE_URL')

if not TELEGRAM_TOKEN:
    logger.error("❌ TELEGRAM_TOKEN environment variable not set!")
    sys.exit(1)

if not DATABASE_URL:
    logger.error("❌ DATABASE_URL environment variable not set!")
    sys.exit(1)

class UserEmailManager:
    def __init__(self):
        self.conn = None
        self.connect_database()
        self.init_database()
    
    def connect_database(self):
        """Connect to PostgreSQL database"""
        try:
            # Parse database URL for Render
            url = urlparse.urlparse(DATABASE_URL)
            dbname = url.path[1:]
            user = url.username
            password = url.password
            host = url.hostname
            port = url.port

            self.conn = psycopg2.connect(
                dbname=dbname,
                user=user,
                password=password,
                host=host,
                port=port,
                sslmode='require'
            )
            logger.info("✅ Connected to PostgreSQL database")
        except Exception as e:
            logger.error(f"❌ Database connection failed: {e}")
            raise
    
    def init_database(self):
        """Initialize PostgreSQL database tables"""
        try:
            cursor = self.conn.cursor()
            
            # Create users table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS users (
                    user_id BIGINT PRIMARY KEY,
                    telegram_chat_id BIGINT UNIQUE,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # Create gmail_accounts table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS gmail_accounts (
                    account_id SERIAL PRIMARY KEY,
                    user_id BIGINT,
                    account_name TEXT,
                    gmail_email TEXT,
                    gmail_password TEXT,
                    is_active BOOLEAN DEFAULT TRUE,
                    setup_complete BOOLEAN DEFAULT FALSE,
                    last_uid INTEGER DEFAULT 0,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (user_id) REFERENCES users (user_id),
                    UNIQUE(user_id, gmail_email)
                )
            ''')
            
            # Create processed_emails table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS processed_emails (
                    email_uid TEXT PRIMARY KEY,
                    account_id INTEGER,
                    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (account_id) REFERENCES gmail_accounts (account_id)
                )
            ''')
            
            self.conn.commit()
            cursor.close()
            logger.info("✅ PostgreSQL tables initialized")
            
        except Exception as e:
            logger.error(f"❌ Database initialization failed: {e}")
            self.conn.rollback()
            raise
    
    def add_user(self, user_id, telegram_chat_id):
        """Add or update user"""
        try:
            cursor = self.conn.cursor()
            cursor.execute('''
                INSERT INTO users (user_id, telegram_chat_id) 
                VALUES (%s, %s)
                ON CONFLICT (user_id) 
                DO UPDATE SET telegram_chat_id = EXCLUDED.telegram_chat_id
            ''', (user_id, telegram_chat_id))
            self.conn.commit()
            cursor.close()
        except Exception as e:
            logger.error(f"❌ Error adding user: {e}")
            self.conn.rollback()
    
    def get_user(self, user_id):
        """Get user by user_id"""
        try:
            cursor = self.conn.cursor()
            cursor.execute('SELECT * FROM users WHERE user_id = %s', (user_id,))
            row = cursor.fetchone()
            cursor.close()
            
            if row:
                return {'user_id': row[0], 'telegram_chat_id': row[1], 'created_at': row[2]}
            return None
        except Exception as e:
            logger.error(f"❌ Error getting user: {e}")
            return None
    
    def validate_gmail_credentials(self, gmail_email, gmail_password):
        """Validate Gmail credentials"""
        try:
            mail = imaplib.IMAP4_SSL("imap.gmail.com")
            mail.login(gmail_email, gmail_password)
            mail.select("inbox")
            mail.logout()
            return True
        except Exception as e:
            logger.error(f"❌ Gmail validation failed: {e}")
            return False
    
    def add_gmail_account(self, user_id, account_name, gmail_email, gmail_password):
        """Add Gmail account for user"""
        if not self.validate_gmail_credentials(gmail_email, gmail_password):
            raise Exception("Invalid Gmail credentials. Please check your email and app password.")
        
        try:
            cursor = self.conn.cursor()
            cursor.execute('''
                INSERT INTO gmail_accounts 
                (user_id, account_name, gmail_email, gmail_password) 
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (user_id, gmail_email) 
                DO UPDATE SET 
                    account_name = EXCLUDED.account_name,
                    gmail_password = EXCLUDED.gmail_password,
                    is_active = TRUE
                RETURNING account_id
            ''', (user_id, account_name, gmail_email, gmail_password))
            
            account_id = cursor.fetchone()[0]
            self.conn.commit()
            cursor.close()
            
            logger.info(f"✅ Added account: {account_name}")
            return account_id
            
        except Exception as e:
            logger.error(f"❌ Error adding Gmail account: {e}")
            self.conn.rollback()
            raise
    
    def mark_setup_complete(self, account_id):
        """Mark account setup as complete"""
        try:
            cursor = self.conn.cursor()
            cursor.execute('UPDATE gmail_accounts SET setup_complete = TRUE WHERE account_id = %s', (account_id,))
            self.conn.commit()
            cursor.close()
        except Exception as e:
            logger.error(f"❌ Error marking setup complete: {e}")
            self.conn.rollback()
    
    def update_last_uid(self, account_id, last_uid):
        """Update last processed UID"""
        try:
            cursor = self.conn.cursor()
            cursor.execute('UPDATE gmail_accounts SET last_uid = %s WHERE account_id = %s', (last_uid, account_id))
            self.conn.commit()
            cursor.close()
        except Exception as e:
            logger.error(f"❌ Error updating last UID: {e}")
            self.conn.rollback()
    
    def get_last_uid(self, account_id):
        """Get last processed UID"""
        try:
            cursor = self.conn.cursor()
            cursor.execute('SELECT last_uid FROM gmail_accounts WHERE account_id = %s', (account_id,))
            result = cursor.fetchone()
            cursor.close()
            return result[0] if result else 0
        except Exception as e:
            logger.error(f"❌ Error getting last UID: {e}")
            return 0
    
    def get_gmail_accounts(self, user_id):
        """Get all Gmail accounts for user"""
        try:
            cursor = self.conn.cursor()
            cursor.execute('SELECT * FROM gmail_accounts WHERE user_id = %s AND is_active = TRUE', (user_id,))
            accounts = []
            for row in cursor.fetchall():
                accounts.append({
                    'account_id': row[0], 'user_id': row[1], 'account_name': row[2],
                    'gmail_email': row[3], 'gmail_password': row[4], 'is_active': row[5],
                    'setup_complete': row[6], 'last_uid': row[7], 'created_at': row[8]
                })
            cursor.close()
            return accounts
        except Exception as e:
            logger.error(f"❌ Error getting Gmail accounts: {e}")
            return []
    
    def get_gmail_account(self, account_id):
        """Get specific Gmail account"""
        try:
            cursor = self.conn.cursor()
            cursor.execute('SELECT * FROM gmail_accounts WHERE account_id = %s', (account_id,))
            row = cursor.fetchone()
            cursor.close()
            if row:
                return {
                    'account_id': row[0], 'user_id': row[1], 'account_name': row[2],
                    'gmail_email': row[3], 'gmail_password': row[4], 'is_active': row[5],
                    'setup_complete': row[6], 'last_uid': row[7], 'created_at': row[8]
                }
            return None
        except Exception as e:
            logger.error(f"❌ Error getting Gmail account: {e}")
            return None
    
    def add_processed_email(self, account_id, email_uid):
        """Mark email as processed"""
        try:
            cursor = self.conn.cursor()
            cursor.execute('INSERT INTO processed_emails (email_uid, account_id) VALUES (%s, %s) ON CONFLICT (email_uid) DO NOTHING', (email_uid, account_id))
            self.conn.commit()
            cursor.close()
        except Exception as e:
            logger.error(f"❌ Error adding processed email: {e}")
            self.conn.rollback()
    
    def is_email_processed(self, account_id, email_uid):
        """Check if email is already processed"""
        try:
            cursor = self.conn.cursor()
            cursor.execute('SELECT 1 FROM processed_emails WHERE account_id = %s AND email_uid = %s', (account_id, email_uid))
            result = cursor.fetchone()
            cursor.close()
            return result is not None
        except Exception as e:
            logger.error(f"❌ Error checking processed email: {e}")
            return False
    
    def delete_gmail_account(self, account_id):
        """Delete Gmail account"""
        try:
            cursor = self.conn.cursor()
            cursor.execute('DELETE FROM processed_emails WHERE account_id = %s', (account_id,))
            cursor.execute('DELETE FROM gmail_accounts WHERE account_id = %s', (account_id,))
            self.conn.commit()
            cursor.close()
            logger.info(f"✅ Deleted account: {account_id}")
        except Exception as e:
            logger.error(f"❌ Error deleting Gmail account: {e}")
            self.conn.rollback()

class LightningGmailBot:
    def __init__(self, telegram_token, chat_id, gmail_email, gmail_password, user_id, account_name, account_id):
        self.bot = Bot(token=telegram_token)
        self.chat_id = chat_id
        self.gmail_email = gmail_email
        self.gmail_password = self.clean_app_password(gmail_password)
        self.user_id = user_id
        self.account_name = account_name
        self.account_id = account_id
        self.user_manager = UserEmailManager()
        self.mail = None
        self.is_running = False
        self.last_uid = 0
        self.initial_high_uid = 0
        logger.info(f"🤖 Initialized: {account_name}")
    
    def clean_app_password(self, password):
        """Clean app password by removing spaces"""
        return password.replace(" ", "")
    
    def connect_gmail(self):
        """Connect and select INBOX - FAST and robust for real-time"""
        try:
            # Check if existing connection is alive (noop is IMAP keep-alive)
            if self.mail:
                try:
                    self.mail.noop() 
                    return True # Connection is alive
                except:
                    self.mail = None # Connection is dead
            
            # Establish new connection
            self.mail = imaplib.IMAP4_SSL("imap.gmail.com", timeout=10)
            self.mail.login(self.gmail_email, self.gmail_password)
            self.mail.select("inbox")
            logger.info(f"🔗 Connected: {self.account_name}")
            return True
        except Exception as e:
            logger.error(f"❌ Connection failed for {self.account_name}: {e}")
            self.mail = None
            return False

    def close_gmail_connection(self):
        """Logout and close connection"""
        if self.mail:
            try:
                self.mail.close()
                self.mail.logout()
                logger.info(f"🛑 Disconnected: {self.account_name}")
            except:
                pass
            finally:
                self.mail = None

    def get_highest_uid(self):
        """Get the highest UID currently in the inbox. Used to skip old emails on first connect."""
        if not self.connect_gmail():
            return 0
        try:
            status, messages = self.mail.uid('search', None, 'ALL')
            if status == "OK" and messages[0]:
                all_uids = messages[0].split()
                if all_uids:
                    return int(all_uids[-1].decode('utf-8'))
            return 0
        except Exception as e:
            logger.error(f"⚠️ Error getting highest UID: {e}")
            return 0
    
    def decode_mime_words(self, text):
        """Decode MIME encoded words"""
        if not text:
            return ""
        try:
            decoded_parts = decode_header(text)
            decoded_text = ""
            for part, encoding in decoded_parts:
                if isinstance(part, bytes):
                    decoded_text += part.decode(encoding or 'utf-8', errors='ignore')
                else:
                    decoded_text += part
            return decoded_text.strip()
        except:
            return str(text)
    
    def clean_telegram_text(self, text):
        """Clean text for Telegram"""
        if not text:
            return ""
        text = re.sub(r'\n\s*\n', '\n\n', text)
        return text
    
    def split_long_message(self, message):
        """Split long message into Telegram-friendly parts"""
        if len(message) <= 4096:
            return [message]
        
        parts = []
        current_part = ""
        
        # Split by lines to maintain readability
        lines = message.split('\n')
        
        for line in lines:
            # If adding this line would exceed the limit, start a new part
            if len(current_part) + len(line) + 1 > 4000:
                if current_part:
                    parts.append(current_part.strip())
                    current_part = ""
            
            current_part += line + '\n'
        
        # Add the last part
        if current_part.strip():
            parts.append(current_part.strip())
        
        return parts
    
    def extract_email_content(self, msg):
        """Extract email content - FAST"""
        plain_text = ""
        html_text = ""
        
        if msg.is_multipart():
            for part in msg.walk():
                content_type = part.get_content_type()
                content_disposition = str(part.get("Content-Disposition", ""))
                
                if "attachment" in content_disposition:
                    continue
                
                if content_type == "text/plain" and not plain_text:
                    try:
                        payload = part.get_payload(decode=True)
                        if payload:
                            charset = part.get_content_charset() or 'utf-8'
                            plain_text = payload.decode(charset, errors='ignore')
                            if plain_text.strip():
                                return self.clean_telegram_text(plain_text.strip())
                    except:
                        continue
                elif content_type == "text/html" and not html_text:
                    try:
                        payload = part.get_payload(decode=True)
                        if payload:
                            charset = part.get_content_charset() or 'utf-8'
                            html_text = payload.decode(charset, errors='ignore')
                    except:
                        continue
        else:
            try:
                payload = msg.get_payload(decode=True)
                if payload:
                    charset = msg.get_content_charset() or 'utf-8'
                    plain_text = payload.decode(charset, errors='ignore')
            except:
                pass
        
        if plain_text and plain_text.strip():
            return self.clean_telegram_text(plain_text.strip())
        elif html_text:
            try:
                soup = BeautifulSoup(html_text, 'html.parser')
                for script in soup(["script", "style"]):
                    script.decompose()
                text = soup.get_text()
                lines = (line.strip() for line in text.splitlines())
                chunks = (phrase.strip() for line in lines for phrase in line.split("  "))
                text = ' '.join(chunk for chunk in chunks if chunk)
                return self.clean_telegram_text(text)
            except:
                return self.clean_telegram_text(html_text)
        
        return "📨 Email received (content extracted)"
    
    def get_email_content(self, email_uid):
        """Get email content by UID - FAST"""
        try:
            status, msg_data = self.mail.uid('fetch', email_uid, "(BODY.PEEK[])")
            if status != "OK" or not msg_data[0]:
                return None
            
            raw_email = msg_data[0][1]
            msg = email.message_from_bytes(raw_email)
            
            subject = self.decode_mime_words(msg.get("subject", "")) or "No Subject"
            sender = self.decode_mime_words(msg.get("from", "")) or "Unknown Sender"
            date = msg.get("date", "Unknown Date")
            to = self.decode_mime_words(msg.get("to", "")) or "Unknown Recipient"
            
            body = self.extract_email_content(msg)
            
            return {
                'subject': subject,
                'sender': sender,
                'date': date,
                'to': to,
                'body': body,
                'email_uid': email_uid
            }
            
        except Exception as e:
            logger.error(f"⚠️ Email fetch error: {e}")
            return None
    
    async def process_new_email(self, email_uid):
        """Process a new email - FAST"""
        if not self.is_running:
            return
            
        if self.user_manager.is_email_processed(self.account_id, email_uid):
            return
        
        logger.info(f"📨 Processing: {self.account_name} - UID {email_uid}")
        
        email_details = self.get_email_content(email_uid)
        
        if email_details:
            message = f"""📧 NEW EMAIL - {self.account_name}

From: {email_details['sender']}
To: {email_details['to']}
Subject: {email_details['subject']}  
Date: {email_details['date']}

Content:
{email_details['body']}"""

            try:
                # ✅ FIX: Use split_long_message for all emails
                message_parts = self.split_long_message(message)
                
                for i, part in enumerate(message_parts, 1):
                    if len(message_parts) > 1:
                        part = f"📧 Part {i}/{len(message_parts)}\n\n{part}"
                    await self.bot.send_message(chat_id=self.chat_id, text=part)
                    await asyncio.sleep(0.5)  # Small delay between parts
                
                logger.info(f"✅ Forwarded: {email_details['subject'][:50]}...")
                self.user_manager.add_processed_email(self.account_id, email_uid)
                    
            except Exception as e:
                logger.error(f"❌ Send failed: {e}")
                try:
                    simple_msg = f"📧 New email from {email_details['sender']}\nSubject: {email_details['subject']}"
                    await self.bot.send_message(chat_id=self.chat_id, text=simple_msg)
                    self.user_manager.add_processed_email(self.account_id, email_uid)
                except:
                    pass

    async def get_emails_by_date(self, target_date, is_fast_search=False):
        """Get emails from specific date - FAST or ACCURATE based on request"""
        # Ensure a fresh connection for single-use search
        mail_client = imaplib.IMAP4_SSL("imap.gmail.com", timeout=10)
        try:
            mail_client.login(self.gmail_email, self.gmail_password)
            mail_client.select("inbox")
        except Exception as e:
            logger.error(f"❌ Date search connection failed: {e}")
            return None

        try:
            emails_found = []
            
            if is_fast_search:
                # 1. FAST HEURISTIC (For Today/Yesterday) - Check last 50 emails
                logger.info(f"🔍 FAST Searching date (last 50 UIDs)")
                # Use ALL and then check last 50 UIDs
                status, messages = mail_client.uid('search', None, "ALL") 
                
                if status == "OK" and messages[0]:
                    all_uids = messages[0].split()
                    # Check up to last 50 emails to cover recent history quickly
                    recent_uids = all_uids[-50:] 
                    
                    for email_uid in recent_uids:
                        uid_str = email_uid.decode('utf-8')
                        # Temporarily override self.mail for get_email_content to use mail_client
                        original_mail = self.mail
                        self.mail = mail_client
                        email_details = self.get_email_content(uid_str)
                        self.mail = original_mail
                        
                        if email_details:
                            try:
                                # Strict check on the header date
                                email_date = parsedate_to_datetime(email_details['date']).date()
                                if email_date == target_date: 
                                    emails_found.append(email_details)
                                    if len(emails_found) >= 20: 
                                        break
                            except:
                                continue
                
            else:
                # 2. ACCURATE DATE SEARCH (For Custom Date) - Use ON for 100% accuracy
                date_str = target_date.strftime("%d-%b-%Y")
                
                # Using 'ON' search is the most accurate for finding emails on a specific date.
                search_criteria = f'ON "{date_str}"'
                
                status, messages = mail_client.uid('search', None, search_criteria)
                
                if status == "OK" and messages[0]:
                    email_uids = messages[0].split()
                    logger.info(f"📬 Found {len(email_uids)} potential emails using ACCURATE IMAP search for {date_str}")
                    
                    # Process maximum 50 emails from the accurate IMAP search result
                    for email_uid in email_uids[:50]: 
                        uid_str = email_uid.decode('utf-8')
                        # Temporarily override self.mail for get_email_content to use mail_client
                        original_mail = self.mail
                        self.mail = mail_client
                        email_details = self.get_email_content(uid_str)
                        self.mail = original_mail
                        
                        if email_details:
                            try:
                                # Secondary, strict check on the header date to guarantee 100% accuracy
                                email_date = parsedate_to_datetime(email_details['date']).date()
                                if email_date == target_date: 
                                    emails_found.append(email_details)
                                
                            except Exception as e:
                                logger.error(f"⚠️ Error parsing date for UID {uid_str}: {e}")
                                continue
            
            logger.info(f"✅ Found {len(emails_found)} emails for {target_date}")
            return emails_found
                    
        except Exception as e:
            logger.error(f"❌ IMAP search error: {e}")
            return None
        finally:
            # Ensure the single-use connection is closed
            try:
                mail_client.close()
                mail_client.logout()
            except:
                pass

    async def run_lightning(self):
        """Real-time email monitoring - SUPER FAST and ROBUST"""
        logger.info(f"🚀 STARTING: {self.account_name}")
        
        # ✅ FIX: Get current highest UID and set as starting point
        if not self.connect_gmail():
            logger.error(f"❌ Initial connection failed for {self.account_name}")
            return
            
        # Get current highest UID to skip all existing emails
        current_highest_uid = self.get_highest_uid()
        if current_highest_uid > 0:
            self.last_uid = current_highest_uid
            self.user_manager.update_last_uid(self.account_id, self.last_uid)
            logger.info(f"✅ Skipped old emails. Starting from UID: {self.last_uid}")
        else:
            # Fallback to database last UID
            self.last_uid = self.user_manager.get_last_uid(self.account_id)
            logger.info(f"📊 Using last UID from DB: {self.last_uid}")
        
        self.is_running = True
        check_count = 0
        
        try:
            while self.is_running:
                
                # RECONNECT / KEEP-ALIVE
                if not self.connect_gmail():
                    await asyncio.sleep(10) # Wait longer if connection fails
                    continue 

                check_count += 1
                
                try:
                    # Search for NEW emails - FAST (Use UID search from last_uid + 1)
                    search_criteria = f"UID {self.last_uid + 1}:*"
                    
                    status, messages = self.mail.uid('search', None, search_criteria)
                    
                    if status == "OK" and messages[0]:
                        email_uids = messages[0].split()
                        
                        if email_uids:
                            logger.info(f"🔍 Found {len(email_uids)} new emails")
                        
                        # Process emails quickly
                        for email_uid in email_uids:
                            if not self.is_running:
                                break
                            
                            uid_str = email_uid.decode('utf-8')
                            uid_num = int(uid_str)
                            
                            if uid_num > self.last_uid:
                                await self.process_new_email(uid_str)
                                self.last_uid = uid_num
                                self.user_manager.update_last_uid(self.account_id, self.last_uid)
                    
                    else:
                        if check_count % 100 == 0:
                            logger.info(f"⏳ No new emails - Check #{check_count}")
                    
                except imaplib.IMAP4.error as e:
                    # Specific IMAP error (e.g., connection lost)
                    logger.error(f"🔄 IMAP error in loop: {e}. Reconnecting...")
                    self.close_gmail_connection()
                except Exception as e:
                    logger.error(f"🔄 General error in loop: {e}. Reconnecting...")
                    self.close_gmail_connection()
                
                # FAST CHECKING - 2 second interval
                await asyncio.sleep(2)
                
        except Exception as e:
            logger.error(f"❌ Monitoring loop error: {e}")
        finally:
            self.is_running = False
            self.close_gmail_connection()
            logger.info(f"🛑 Stopped: {self.account_name}")

class MultiUserEmailBot:
    def __init__(self, telegram_token):
        self.telegram_token = telegram_token
        self.user_manager = UserEmailManager()
        self.active_bots = {}
        self.application = None
        self.bot_instance = Bot(token=telegram_token)
    
    def create_main_menu(self):
        """Create main menu"""
        keyboard = [
            [InlineKeyboardButton("📧 My Mails", callback_data="my_mails")],
            [InlineKeyboardButton("🔗 Connect Mails", callback_data="connect_mails")],
            [InlineKeyboardButton("🗑️ Remove Mails", callback_data="remove_mails")],
            [InlineKeyboardButton("ℹ️ How to Connect", callback_data="how_to_connect")]
        ]
        return InlineKeyboardMarkup(keyboard)
    
    def setup_handlers(self, application):
        """Setup Telegram bot handlers"""
        application.add_handler(CommandHandler("start", self.start_command))
        application.add_handler(CallbackQueryHandler(self.handle_callback))
        application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, self.handle_message))
        logger.info("✅ Bot handlers setup")
    
    async def start_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /start command"""
        user_id = update.effective_user.id
        chat_id = update.effective_chat.id
        self.user_manager.add_user(user_id, chat_id)
        
        welcome_text = """Secure Gmail Bot

📧 Features:
• Connect multiple Gmail accounts
• Real-time email forwarding (FAST)
• View emails by specific date

🔐 Security:
• Only NEW emails forwarded after setup
• Credentials validated before saving

Choose an option below:"""
        
        await update.message.reply_text(welcome_text, reply_markup=self.create_main_menu())
    
    async def show_main_menu(self, update: Update):
        """Show main menu"""
        if hasattr(update, 'callback_query'):
            try:
                await update.callback_query.edit_message_text("🤖 Multi-Gmail Email Bot\n\nChoose an option:", reply_markup=self.create_main_menu())
            except:
                pass
    
    async def show_my_mails_menu(self, update: Update, user_id):
        """Show my mails menu"""
        accounts = self.user_manager.get_gmail_accounts(user_id)
        
        if not accounts:
            await update.callback_query.edit_message_text(
                "❌ No Gmail accounts yet.\nUse 'Connect Mails' to add one.",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("🔗 Connect Mails", callback_data="connect_mails")],
                    [InlineKeyboardButton("🔙 Back", callback_data="back_to_main")]
                ])
            )
            return
        
        keyboard = []
        for account in accounts:
            status = "🟢" if account.get('setup_complete') else "🟡"
            keyboard.append([InlineKeyboardButton(f"{status} {account['account_name']}", callback_data=f"account_{account['account_id']}")])
        
        keyboard.append([InlineKeyboardButton("🔙 Back", callback_data="back_to_main")])
        
        await update.callback_query.edit_message_text("📧 Your Accounts:\nSelect an account:", reply_markup=InlineKeyboardMarkup(keyboard))
    
    async def show_mail_options(self, update: Update, account_id):
        """Show mail options"""
        account = self.user_manager.get_gmail_account(account_id)
        if not account:
            await update.callback_query.edit_message_text("❌ Account not found!")
            return
        
        status = "Active" if account.get('setup_complete') else "Setting up"
        
        keyboard = [
            [InlineKeyboardButton("⚡ Realtime Mails", callback_data=f"realtime_{account_id}")],
            [InlineKeyboardButton("📂 Old Mails", callback_data=f"old_mails_menu_{account_id}")], 
            [InlineKeyboardButton("🔙 Back", callback_data="my_mails")]
        ]
        
        await update.callback_query.edit_message_text(f"📧 {account['account_name']}\n{account['gmail_email']}\nStatus: {status}\n\nChoose:", reply_markup=InlineKeyboardMarkup(keyboard))
    
    async def show_old_mails_menu(self, update: Update, account_id):
        """Show specific options for old mails"""
        account = self.user_manager.get_gmail_account(account_id)
        if not account:
            await update.callback_query.edit_message_text("❌ Account not found!")
            return
            
        keyboard = [
            [InlineKeyboardButton("📅 Today", callback_data=f"old_mails_date_today_{account_id}")],
            [InlineKeyboardButton("⬅️ Yesterday", callback_data=f"old_mails_date_yesterday_{account_id}")],
            [InlineKeyboardButton("🗓️ Custom Date", callback_data=f"old_mails_date_custom_{account_id}")],
            [InlineKeyboardButton("🔙 Back", callback_data=f"account_{account_id}")]
        ]

        await update.callback_query.edit_message_text(f"📂 Old Mails - {account['account_name']}\n\nSelect a date or enter a custom one:", reply_markup=InlineKeyboardMarkup(keyboard))
    
    async def start_realtime_monitoring(self, update: Update, account_id):
        """Start realtime monitoring"""
        account = self.user_manager.get_gmail_account(account_id)
        if not account:
            await update.callback_query.edit_message_text("❌ Account not found!")
            return
        
        if account['user_id'] in self.active_bots:
            for bot in self.active_bots[account['user_id']]:
                if bot.gmail_email == account['gmail_email'] and bot.is_running:
                    await update.callback_query.edit_message_text(f"✅ Already Monitoring!\n{account['account_name']}\nReal-time is active!", reply_markup=InlineKeyboardMarkup([
                        [InlineKeyboardButton("🔙 Back", callback_data=f"account_{account_id}")],
                        [InlineKeyboardButton("🏠 Main", callback_data="back_to_main")]
                    ]))
                    return
        
        await self.start_monitoring_account(account['user_id'], account['gmail_email'])
        
        await update.callback_query.edit_message_text(f"✅ FAST Monitoring Started!\n{account['account_name']}\nYou'll receive new emails in real-time.", reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("🔙 Back", callback_data=f"account_{account_id}")],
            [InlineKeyboardButton("🏠 Main", callback_data="back_to_main")]
        ]))

    async def ask_for_custom_date(self, update: Update, context: ContextTypes.DEFAULT_TYPE, account_id):
        """Ask for custom date"""
        account = self.user_manager.get_gmail_account(account_id)
        if not account:
            await update.callback_query.edit_message_text("❌ Account not found!")
            return
        
        context.user_data['selected_account_id'] = account_id
        context.user_data['awaiting_custom_date'] = True
        
        await update.callback_query.edit_message_text(
            f"🗓️ Custom Date Search - {account['account_name']}\n\n"
            f"Enter date (DDMMYYYY, DD-MM-YYYY, or DD/MM/YYYY):", 
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data=f"old_mails_menu_{account_id}")]])
        )
    
    async def process_predefined_date_search(self, update: Update, context: ContextTypes.DEFAULT_TYPE, date_type, account_id):
        """Process Today/Yesterday search from buttons"""
        if date_type == 'today':
            target_date = datetime.now().date()
            date_display = "today"
            is_fast_search = True
        elif date_type == 'yesterday':
            target_date = (datetime.now() - timedelta(days=1)).date()
            date_display = "yesterday"
            is_fast_search = True
        else:
            await update.callback_query.edit_message_text("❌ Invalid date type.")
            return

        context.user_data['selected_account_id'] = account_id
        
        chat_id = update.effective_chat.id
        await update.callback_query.delete_message()
        await self.execute_date_search(chat_id, context, target_date, date_display, is_fast_search)

    async def handle_callback(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle button callbacks"""
        query = update.callback_query
        try:
            await query.answer()
        except:
            pass
        
        callback_data = query.data
        user_id = query.from_user.id
        
        try:
            if callback_data == "back_to_main":
                await self.show_main_menu(update)
            elif callback_data == "my_mails":
                await self.show_my_mails_menu(update, user_id)
            elif callback_data == "connect_mails":
                await self.start_account_setup(update, context)
            elif callback_data == "remove_mails":
                await self.show_remove_accounts_menu(update, user_id)
            elif callback_data == "how_to_connect":
                await self.show_how_to_connect(update)
            elif callback_data.startswith('account_'):
                account_id = int(callback_data.split('_')[1])
                await self.show_mail_options(update, account_id)
            elif callback_data.startswith('old_mails_menu_'):
                account_id = int(callback_data.split('_')[-1])
                await self.show_old_mails_menu(update, account_id)
            elif callback_data.startswith('old_mails_date_today_'):
                account_id = int(callback_data.split('_')[-1])
                await self.process_predefined_date_search(update, context, 'today', account_id)
            elif callback_data.startswith('old_mails_date_yesterday_'):
                account_id = int(callback_data.split('_')[-1])
                await self.process_predefined_date_search(update, context, 'yesterday', account_id)
            elif callback_data.startswith('old_mails_date_custom_'):
                account_id = int(callback_data.split('_')[-1])
                await self.ask_for_custom_date(update, context, account_id)
            elif callback_data.startswith('realtime_'):
                account_id = int(callback_data.split('_')[1])
                await self.start_realtime_monitoring(update, account_id)
            elif callback_data.startswith('delete_'):
                account_id = int(callback_data.split('_')[1])
                await self.delete_account(update, account_id)
                
        except Exception as e:
            logger.error(f"❌ Callback error: {e}")
            try:
                await self.bot_instance.send_message(chat_id=query.message.chat_id, text="❌ An error occurred. Please try again.")
            except:
                pass
    
    async def show_remove_accounts_menu(self, update: Update, user_id):
        """Show remove accounts menu"""
        accounts = self.user_manager.get_gmail_accounts(user_id)
        
        if not accounts:
            await update.callback_query.edit_message_text("❌ No accounts to remove.")
            return
        
        keyboard = []
        for account in accounts:
            keyboard.append([InlineKeyboardButton(f"🗑️ {account['account_name']}", callback_data=f"delete_{account['account_id']}")])
        
        keyboard.append([InlineKeyboardButton("🔙 Back", callback_data="back_to_main")])
        
        await update.callback_query.edit_message_text("🗑️ Remove Accounts:", reply_markup=InlineKeyboardMarkup(keyboard))
    
    async def delete_account(self, update: Update, account_id):
        """Delete account"""
        account = self.user_manager.get_gmail_account(account_id)
        if not account:
            await update.callback_query.edit_message_text("❌ Account not found!")
            return
        
        await self.stop_monitoring_account(account['user_id'], account['gmail_email'])
        self.user_manager.delete_gmail_account(account_id)
        
        await update.callback_query.edit_message_text(f"✅ Deleted: {account['account_name']}", reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("🔙 Back", callback_data="remove_mails")],
            [InlineKeyboardButton("🏠 Main", callback_data="back_to_main")]
        ]))
    
    async def show_how_to_connect(self, update: Update):
        """Show how to connect"""
        instructions = """🔗 How to Connect:

1. Enable 2-Factor Authentication
2. Create App Password:
   - Google Account → Manage Account → (Search) App passwords!
   - Select Mail → Chouse any name → Generate
   - Copy 16-character password
3. Use 'Connect Mails' button
4. Enter name, email, and app password

🔒 Security: Your credentials are validated before saving"""

        await update.callback_query.edit_message_text(instructions, reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("🔗 Connect", callback_data="connect_mails")],
            [InlineKeyboardButton("🔙 Back", callback_data="back_to_main")]
        ]))
    
    async def start_account_setup(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Start account setup"""
        context.user_data['setup_step'] = 'awaiting_account_name'
        await update.callback_query.edit_message_text("🔗 Connect Gmail Account\n\nStep 1: Enter account name:", reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("🔙 Cancel", callback_data="back_to_main")]
        ]))
    
    async def handle_message(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle text messages"""
        user_id = update.effective_user.id
        text = update.message.text.strip()
        
        if 'setup_step' in context.user_data:
            await self.handle_setup_steps(update, context, text, user_id)
        elif context.user_data.get('awaiting_custom_date'):
            await self.handle_custom_date_input(update, context, text)

    async def handle_setup_steps(self, update: Update, context: ContextTypes.DEFAULT_TYPE, text: str, user_id: int):
        """Handle setup steps"""
        setup_step = context.user_data['setup_step']
        
        if setup_step == 'awaiting_account_name':
            if len(text) < 2:
                await update.message.reply_text("❌ Enter valid name (2+ characters):")
                return
            
            context.user_data['account_name'] = text
            context.user_data['setup_step'] = 'awaiting_gmail_email'
            await update.message.reply_text("✅ Name saved\nStep 2: Enter Gmail address:")
        
        elif setup_step == 'awaiting_gmail_email':
            if not re.match(r'^[a-zA-Z0-9._%+-]+@gmail\.com$', text):
                await update.message.reply_text("❌ Enter valid Gmail address:")
                return
            
            context.user_data['gmail_email'] = text
            context.user_data['setup_step'] = 'awaiting_gmail_password'
            await update.message.reply_text("✅ Email saved\nStep 3: Enter 16-character App Password:")
        
        elif setup_step == 'awaiting_gmail_password':
            cleaned_password = text.replace(" ", "")
            
            if len(cleaned_password) != 16 or not re.match(r'^[a-zA-Z0-9]{16}$', cleaned_password):
                await update.message.reply_text("❌ Enter valid 16-character App Password:")
                return
            
            account_name = context.user_data['account_name']
            gmail_email = context.user_data['gmail_email']
            gmail_password = cleaned_password
            
            try:
                validating_msg = await update.message.reply_text("🔐 Validating Gmail credentials and skipping old emails...")
                
                # 1. Add account (which validates credentials)
                account_id = self.user_manager.add_gmail_account(user_id, account_name, gmail_email, gmail_password)
                
                # 2. Temporary bot to get current highest UID and mark it as 'last_uid'
                temp_bot = LightningGmailBot(
                    self.telegram_token,
                    update.effective_chat.id,
                    gmail_email,
                    gmail_password,
                    user_id,
                    account_name,
                    account_id
                )
                
                # **FAST BLOCK: Mark all existing emails as processed**
                highest_uid = temp_bot.get_highest_uid()
                self.user_manager.update_last_uid(account_id, highest_uid)
                self.user_manager.mark_setup_complete(account_id)
                logger.info(f"✅ Skipped old emails for {account_name}. Last UID set to: {highest_uid}")
                
                # 3. Close the temporary connection
                temp_bot.close_gmail_connection()
                        
                await validating_msg.edit_text("✅ Account setup complete! Starting real-time monitoring...")
                
                await self.start_monitoring_account(user_id, gmail_email)
                
                context.user_data.clear()
                
                await update.message.reply_text(f"✅ Setup Complete!\n{account_name}\nOnly **new** emails will be forwarded in real-time.", reply_markup=self.create_main_menu())
                
            except Exception as e:
                logger.error(f"❌ Setup error: {e}")
                await update.message.reply_text(f"❌ Setup failed: {str(e)}\n\nPlease try again:", reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("🔄 Retry", callback_data="connect_mails")],
                    [InlineKeyboardButton("🏠 Main", callback_data="back_to_main")]
                ]))
    
    def parse_flexible_date(self, date_string):
        """Parse multiple DDMMYYYY date formats (DDMMYYYY, DD-MM-YYYY, DD/MM/YYYY)"""
        date_string = date_string.replace('/', '').replace('-', '').replace('.', '')
        
        if len(date_string) == 8 and date_string.isdigit():
            try:
                return datetime.strptime(date_string, "%d%m%Y").date()
            except ValueError:
                pass
        
        # Also try YYYY-MM-DD for standard users
        try:
            return datetime.strptime(date_string, "%Y-%m-%d").date()
        except ValueError:
            pass
            
        raise ValueError("Invalid date format. Use DDMMYYYY, DD-MM-YYYY, or DD/MM/YYYY.")

    async def handle_custom_date_input(self, update: Update, context: ContextTypes.DEFAULT_TYPE, text: str):
        """Handle custom date input and trigger search"""
        account_id = context.user_data['selected_account_id']
        account = self.user_manager.get_gmail_account(account_id)
        
        if not account:
            await update.message.reply_text("❌ Account not found!")
            context.user_data.clear()
            return
        
        try:
            target_date = self.parse_flexible_date(text)
            date_display = f"{target_date.strftime('%d-%m-%Y')}"
            
            if target_date > datetime.now().date():
                await update.message.reply_text("❌ Cannot search future dates!")
                return
                
        except ValueError as e:
            await update.message.reply_text(f"❌ Invalid date format!\n\n{e}")
            return
        
        context.user_data.pop('awaiting_custom_date', None)
        await self.execute_date_search(update.effective_chat.id, context, target_date, date_display, is_fast_search=False, message_update=update)

    async def execute_date_search(self, chat_id, context: ContextTypes.DEFAULT_TYPE, target_date, date_display, is_fast_search, message_update=None):
        """Execute the final date search logic, supporting both callback and message context"""
        account_id = context.user_data['selected_account_id']
        account = self.user_manager.get_gmail_account(account_id)
        
        if message_update:
            searching_msg = await message_update.message.reply_text(f"🔍 Searching emails for {date_display}...")
            send_final_reply = lambda text, markup=None: message_update.message.reply_text(text, reply_markup=markup)
            edit_searching_msg = lambda text: searching_msg.edit_text(text)
            send_individual_mail = lambda text: message_update.message.reply_text(text)
        else:
            searching_msg = await self.bot_instance.send_message(chat_id=chat_id, text=f"🔍 Searching emails for {date_display}...")
            send_final_reply = lambda text, markup=None: self.bot_instance.send_message(chat_id=chat_id, text=text, reply_markup=markup)
            edit_searching_msg = lambda text: searching_msg.edit_text(text)
            send_individual_mail = lambda text: self.bot_instance.send_message(chat_id=chat_id, text=text)

        try:
            bot = LightningGmailBot(
                self.telegram_token,
                chat_id,
                account['gmail_email'],
                account['gmail_password'],
                account['user_id'],
                account['account_name'],
                account_id
            )
            
            emails = await bot.get_emails_by_date(target_date, is_fast_search=is_fast_search)
            
            if emails is None:
                await edit_searching_msg("❌ Error connecting to Gmail. Please check your credentials or try again later.")
                return
            
            if not emails:
                await edit_searching_msg(f"📭 No emails found for {date_display}")
                return
            
            await edit_searching_msg(f"✅ Found {len(emails)} emails for {date_display}\nSending FAST...")
            
            success_count = 0
            for i, email_details in enumerate(emails, 1):
                try:
                    message = f"""📧 Email {i}/{len(emails)} - {date_display}

From: {email_details['sender']}
To: {email_details['to']}
Subject: {email_details['subject']}
Date: {email_details['date']}

Content:
{email_details['body']}"""
                    
                    message_parts = bot.split_long_message(message)
                    
                    for part_num, part in enumerate(message_parts, 1):
                        if len(message_parts) > 1:
                            part = f"📧 Part {part_num}/{len(message_parts)}\n\n{part}"
                        await send_individual_mail(part)
                        await asyncio.sleep(0.1)
                    
                    success_count += 1
                    await asyncio.sleep(0.1)
                    
                except Exception as e:
                    logger.error(f"❌ Error sending email {i}: {e}")
                    try:
                        simple_msg = f"📧 {i}/{len(emails)} - {email_details['sender']}\nSubject: {email_details['subject'][:100]}..."
                        await send_individual_mail(simple_msg)
                        success_count += 1
                    except:
                        continue
            
            await send_final_reply(f"✅ Sent {success_count} emails from {date_display}", markup=self.create_main_menu())
            
        except Exception as e:
            logger.error(f"❌ Date search execution error: {e}")
            await edit_searching_msg(f"❌ Error: {str(e)}")
        
        context.user_data.clear()
    
    async def start_monitoring_account(self, user_id, gmail_email):
        """Start monitoring account"""
        account = None
        accounts = self.user_manager.get_gmail_accounts(user_id)
        for acc in accounts:
            if acc['gmail_email'] == gmail_email:
                account = acc
                break
        
        if not account:
            logger.error(f"❌ Account not found: {gmail_email}")
            return
        
        await self.stop_monitoring_account(user_id, gmail_email)
        
        user = self.user_manager.get_user(user_id)
        if not user:
            logger.error(f"❌ User not found: {user_id}")
            return
        
        bot = LightningGmailBot(
            self.telegram_token,
            user['telegram_chat_id'],
            account['gmail_email'],
            account['gmail_password'],
            user_id,
            account['account_name'],
            account['account_id']
        )
        
        if user_id not in self.active_bots:
            self.active_bots[user_id] = []
        self.active_bots[user_id].append(bot)
        
        asyncio.create_task(bot.run_lightning())
        logger.info(f"🚀 Started FAST monitoring: {account['account_name']}")
    
    async def stop_monitoring_account(self, user_id, gmail_email):
        """Stop monitoring account"""
        if user_id in self.active_bots:
            for bot in self.active_bots[user_id][:]:
                if bot.gmail_email == gmail_email:
                    bot.is_running = False
                    self.active_bots[user_id].remove(bot)
                    logger.info(f"🛑 Stopped monitoring: {gmail_email}")
                    break
    
    async def stop_all_monitoring(self):
        """Stop all monitoring"""
        for user_id, bots in self.active_bots.items():
            for bot in bots:
                bot.is_running = False
        self.active_bots.clear()
        logger.info("🛑 Stopped all monitoring")

def main():
    """Main function"""
    print("🚀 Starting Multi-Gmail Email Bot with PostgreSQL...")
    print("📧 Real-time email monitoring")
    print("🐘 PostgreSQL Database")
    print("⚡ Render Deployment")
    print("✅ All features included")
    print("🤖 Bot running...")
    
    try:
        application = Application.builder().token(TELEGRAM_TOKEN).build()
        multi_bot = MultiUserEmailBot(TELEGRAM_TOKEN)
        multi_bot.setup_handlers(application)
        application.run_polling(allowed_updates=Update.ALL_TYPES)
        
    except KeyboardInterrupt:
        print("\n🛑 Bot stopped by user")
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()