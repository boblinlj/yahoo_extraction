import smtplib
import ssl
from email.message import EmailMessage
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from dataclasses import dataclass
from inputs import pwd


@dataclass
class SendEmail:
    subject: str
    content: str

    def _compose_email(self):
        msg = MIMEMultipart()
        msg['Subject'] = self.subject
        msg['From'] = pwd.EMAIL_FROM
        msg['To'] = pwd.EMAIL_TO

        return msg

    def send(self):
        msg = self._compose_email()
        pt1 = MIMEText(self.content, 'html')
        msg.attach(pt1)

        context = ssl.create_default_context()
        with smtplib.SMTP_SSL("smtp.gmail.com", 465, context=context) as server:
            server.login(pwd.EMAIL_FROM, pwd.EMAIL_PASSWORD)
            server.sendmail(msg['To'], msg['From'], msg.as_string())


if __name__ == '__main__':
    html = """
    <html>
      <body>
        <p><b>{title}</b><br>
           {content}<br>
           {data}<br>
        </p>
      </body>
    </html>
    """
    SendEmail(subject=f'Daily job started for 2022-02-01',
              content=html.format(title='Daily Equity job finished for {runtime}',
                                  content="""{'-'*80}\n"""
                                          """{stock_ext.get_failed_extracts}/{stock_ext.no_of_stock} Failed Extractions. <br>"""
                                          """The job made {stock_ext.no_of_web_calls} calls through the internet. <br>"""
                                          """Target Table: yahoo_fundamental<br>"""
                                          """Target Population: YAHOO_STOCK_ALL<br>"""
                                          """Log: {loggerFileName}<br>"""
                                          """{'-'*80}<br>""",
                                  data='')).send()
