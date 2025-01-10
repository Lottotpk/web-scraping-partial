import boto3
from botocore.exceptions import ClientError
from dotenv import load_dotenv

# define emailids port
AWS_SES_REGION = "us-west-2"
load_dotenv()
recipients = ["tirapatr@ntasset.com","saran@ntasset.com","julian@ntasset.com"]
senderemailid = "info-db@portal.ntasset.com"



# DO NOT NAME YOUR FILE email.py
def send_email_to_analyst(msg, SUBJECT):
    receiveremaiidlist = recipients
    # If necessary, replace us-west-2 with the AWS Region you're using for Amazon SES.
    # The subject line for the email.
    #     SUBJECT = "Model Status"
    # The character encoding for the email.
    CHARSET = "UTF-8"

    # Create a new SES resource and specify a region.
    client = boto3.client('ses', region_name=AWS_SES_REGION)

    # Try to send the email.
    try:
        # Provide the contents of the email.
        response = client.send_email(
            Destination={
                'ToAddresses': receiveremaiidlist,
                'CcAddresses': [],
            },
            Message={
                'Body': {
                    'Html': {
                        'Charset': CHARSET,
                        'Data': msg,
                    },
                },
                'Subject': {
                    'Charset': CHARSET,
                    'Data': SUBJECT,
                },
            },
            Source=senderemailid
        )
        # Display an error if something goes wrong.
    except ClientError as e:
        print("ERROR " + str(e.response['Error']['Message']))
    else:
        print("Email sent!")


if __name__ == '__main__':
    import os
    SUBJECT = "[COMPLETED] Daily Broker Research Alert:"
    elasped = 3600
    total_download = 66
    num_report={"Philippines" : 1, "Indonesia" : 2, "Taiwan" : 3, "India" : 4, "Vietnam" : 5, "Thailand" : 6, "China" : 7, "Hong Kong" : 8, "South Korea" : 9, "Malaysia" : 10, "Singapore" : 11}
    msg = f"""<html>
        <head></head>
        <body>
            <p>Dear recipients,</p>
            <p>Your daily broker research download script has successfully executed today.</p>
            <p>Time elapsed (in seconds): {round(elasped, 2)}</p>
            <p>Number of downloaded reports: {total_download}, which contain reports from the following countries:</p>
        """
    msg += "<ul>"
    for country, num in num_report.items():
        msg += f"<li>{country}: {num} ({round(num/total_download, 2)}%)</li>"
    msg += "</ul>"
    msg += "</body>\n</html>"

    recipients = os.getenv("main_email").strip("][").split(",")
    sender = os.getenv("sender_email")
    print(msg)
    send_email_to_analyst(msg, SUBJECT, recipients, sender)
    # send_email_to_analyst(msg, SUBJECT, recipients)
