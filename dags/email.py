from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import timedelta,datetime
import smtplib, ssl
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import pandas as pd
from sqlalchemy import text,create_engine 

DEFAULT_ARGS = {
    'owner': 'Airflow',
    'depends_on_past': False,
    'start_date': datetime(2022, 6, 10),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG('Send_statistics', default_args=DEFAULT_ARGS,
          schedule_interval="@daily")


DB_cred ="postgresql://postgres:0000@127.0.0.1:5432/BTC_stats"

def select_sql_df(cred: str, query: list):
    temp_list = []
    list = ""
    cnx = create_engine(cred)
    df = pd.read_sql_query(query,con=cnx)
    html = df.to_html()
    html_text = f"""<p> Bitcoin stats from different exchanges </p>
    {html}"""
    temp_list.append(html_text)
    list = " ".join(temp_list)
    return list

    
def send_email_basic(sender, receiver, email_subject, htmls):
    port = 465 
    smtp_server = "smtp.gmail.com"
    sender_email = sender  
    receiver_email = receiver  
    password = "IamTHEking3244"
    email_html = f"""<html>
    <body>
        {htmls}
        <br>
    </body>
    </html>"""
    message = MIMEMultipart("multipart")
    part2 = MIMEText(email_html, "html")
    message.attach(part2)
    message["Subject"] = email_subject
    message["From"] = sender_email

    for i, val in enumerate(receiver):
        message["To"] = val

    context = ssl.create_default_context()
    with smtplib.SMTP_SSL(smtp_server, port, context=context) as server:
        server.login(sender_email, password)
        server.sendmail(sender_email, receiver_email, message.as_string())


def send_email(ds,**_):
    sender = "mamatova2016.9108620@student.karazin.ua" 
    recipients = ["gameampere@gmail.com"]  
    subject = (
        f"Bitfinex,Bitmex,Poloniex statistic for {ds}"  
    )
    query="""
            SELECT DISTINCT exchange,type,extract(hour from timestamp) as Hour, AVG(price)OVER(PARTITION BY exchange,type,extract(hour from timestamp) ORDER by extract(hour from timestamp)) as AvrPrice,
            AVG(amount)OVER(PARTITION BY exchange,type,extract(hour from timestamp) ORDER by extract(hour from timestamp)) as AvrAmount,
            MAX(price)OVER(PARTITION BY exchange,type,extract(hour from timestamp) ORDER by extract(hour from timestamp)) as MaxPrice,
            MAX(amount)OVER(PARTITION BY exchange,type,extract(hour from timestamp) ORDER by extract(hour from timestamp)) as MaxAmount
            From statsbtc
            ORDER BY extract(hour from timestamp)
            """ 
    htmls = select_sql_df(
        DB_cred,query,
    )
    send_email_basic(sender, recipients, subject, htmls)
    
    engine = create_engine(DB_cred)
    connection = engine.connect()
    query = text("TRUNCATE TABLE statsbtc")
    connection.execution_options(autocommit=True).execute(query) 


t1 = PythonOperator(task_id="send_email",
                    provide_context=True,
                    python_callable=send_email,
                    dag=dag)