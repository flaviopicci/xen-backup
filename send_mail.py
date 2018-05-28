import argparse
import json
import logging.config
import os
import smtplib
import sys
from email.message import EmailMessage

import yaml

if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--config", type=str, help="Set configuration file", default="config.yml")

    args = parser.parse_args()
    config_filename = args.config

    logging.config.fileConfig("log.conf")
    logger = logging.getLogger("Xen backup")

    try:
        with open(config_filename, "r") as config_file:
            config = yaml.load(config_file)
    except Exception as ge:
        logger.error("Error opening config file : %s", ge)
        sys.exit(1)

    body = ""
    # Open the plain text file whose name is in textfile for reading.
    with open(config["mail"]["content"]) as mail_file:
        mail = json.load(mail_file)
        subject = mail["subject"]
        for pool_name, pool_errors in mail["body"].items():
            body = body + pool_name + os.linesep + os.linesep
            body = body + "Backup errors:" + os.linesep + "\t" + (os.linesep + "\t").join(
                pool_errors["errors"]) + os.linesep
            body = body + "VMs export errors:" + os.linesep + "\t" + (os.linesep + "\t").join(pool_errors["vms"])

    # Create a text/plain message
    msg = EmailMessage()
    msg.set_content(body)
    msg['Subject'] = subject
    msg['From'] = config["mail"]["from"]
    msg['To'] = config["mail"]["to"]

    # Send the message via a SMTP server.
    mail_server = smtplib.SMTP(config["mail"]["host"], config["mail"]["port"])
    mail_server.esmtp_features['auth'] = 'LOGIN'
    mail_server.ehlo()
    mail_server.starttls()  # enable TLS
    mail_server.ehlo()
    mail_server.login(config["mail"]["user"], config["mail"]["password"])
    mail_server.send_message(msg)
    mail_server.quit()
