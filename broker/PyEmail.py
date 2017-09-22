#http://naelshiab.com/tutorial-send-email-python/
#send email to multi receivers
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

COMMASPACE = ', '
fromaddr = "ideamaxwu@gmail.com"
toaddres = ["ideamaxwu@gmail.com","yao.wu@uci.edu"]
msg = MIMEMultipart()
msg['From'] = fromaddr
msg['To'] = COMMASPACE.join(toaddres)
msg['Subject'] = "SUBJECT OF THE MAIL"
 
body = "YOUR MESSAGE HERE"
msg.attach(MIMEText(body, 'plain'))
 
server = smtplib.SMTP('smtp.gmail.com', 587)
server.starttls()
server.login(fromaddr, "PASSWORD")
text = msg.as_string()
server.sendmail(fromaddr, toaddres, text)
server.quit()