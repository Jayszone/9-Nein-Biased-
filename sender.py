import smtplib
import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText


def send_newsletter(html_content, recipients, gmail_user, gmail_app_password):
    """
    Send the newsletter via Gmail SMTP.

    Requirements:
    - gmail_user: your Gmail address (e.g. yourname@gmail.com)
    - gmail_app_password: a Gmail App Password (not your regular password)
      Generate one at: https://myaccount.google.com/apppasswords
    - recipients: list of email addresses to send to
    """
    today = datetime.date.today().strftime("%B %d, %Y")
    subject = f"9 (Nein) Biased — {today}"

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = f"9 (Nein) Biased <{gmail_user}>"
    msg["To"] = ", ".join(recipients)

    # Attach HTML content
    part = MIMEText(html_content, "html")
    msg.attach(part)

    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
            server.login(gmail_user, gmail_app_password)
            server.sendmail(gmail_user, recipients, msg.as_string())
        print(f"✓ Newsletter sent to {len(recipients)} recipient(s)")
        return True
    except smtplib.SMTPAuthenticationError:
        print("✗ Gmail authentication failed.")
        print("  → Make sure you're using an App Password, not your Gmail password.")
        print("  → Generate one at: https://myaccount.google.com/apppasswords")
        return False
    except Exception as e:
        print(f"✗ Failed to send email: {e}")
        return False
