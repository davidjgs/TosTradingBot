##################################################################################
# GMAIL CLASS ####################################################################
# Handles email auth and messages ################################################

# imports
import datetime
import os

import schedule
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
import os.path
from google.oauth2.credentials import Credentials

from ttb.db.db_persist import DBPersister
from ttb.event.inbound.event_parser import parse_event
from ttb.trading.td_client import TosTrader

from ttb.cfg.config import Config
from queue import Queue
import time
import dateutil.parser

import logging

from ttb.util import timeutil


class GmailReader:

    def __init__(self, config: Config, event_q: Queue, persister: DBPersister = None):
        self.conf = config or Config()
        self.event_q = event_q
        self.persister = persister
        self.logger = logging.getLogger(__name__)
        self.SCOPES = ['https://mail.google.com/']
        self.creds = None
        self.service = None
        self.mail_pull_interval_seconds = self.conf.mail_pull_interval_seconds
        self.cut_off_time_str = self.conf.trade_end_time
        self.cut_off_time = timeutil.parse_time(self.conf.trade_end_time)
        self.trade_start_time = timeutil.parse_time(self.conf.trade_start_time)
        self.token_file = self.conf.mail_token_path
        self.creds_file = self.conf.mail_cred_path
        self.alert_sender = self.conf.mail_alert_sender
        self.__processed_msg_ids = set()

    def start(self):
        if self.__connect():
            self.logger.info("getting mails ...")
            schedule.every(self.mail_pull_interval_seconds).seconds.until(self.cut_off_time_str).do(self.get_mails)
            while datetime.datetime.now().timestamp() < self.cut_off_time.timestamp():
                n = schedule.idle_seconds()
                if n and n > 0:
                    self.logger.info(f'sleeping for {n} seconds ...')
                    time.sleep(n)
                schedule.run_pending()

    def __connect(self):
        try:
            self.logger.info("CONNECTING TO GMAIL...")

            if os.path.exists(self.token_file):
                with open(self.token_file, 'r') as token:
                    self.creds = Credentials.from_authorized_user_file(
                        self.token_file, self.SCOPES)

            if not self.creds:

                flow = InstalledAppFlow.from_client_secrets_file(
                    self.creds_file, self.SCOPES)

                self.creds = flow.run_local_server(port=0)

            elif self.creds and self.creds.expired and self.creds.refresh_token:

                self.creds.refresh(Request())

            if self.creds is not None:

                # Save the credentials for the next run
                with open(self.token_file, 'w') as token:

                    token.write(self.creds.to_json())

                self.service = build('gmail', 'v1', credentials=self.creds)

                self.logger.info("CONNECTED TO GMAIL!\n")

                return True

            else:

                raise Exception("Creds Not Found!")

        except Exception as e:
            print(e)
            self.logger.error("FAILED TO CONNECT TO GMAIL!\n")

            return False

    def __process_mails(self, payloads):
        for payload in payloads:
            if self.persister:
                self.persister.insert_event(payload)
            subject = payload["subject"]
            ts = payload["ts"]
            if self.valid_alert(payload):
                events = parse_event(subject)
                for event in events:
                    self.event_q.put((event[0], event[1], event[2], event[3], ts))

    def valid_alert(self, content):
        sender = content["sender"]
        subject = content["subject"]
        ts = content["ts"]
        dt = datetime.datetime.fromtimestamp(dateutil.parser.parse(ts).timestamp())
        return sender == self.alert_sender and "Alert" in subject and dt.timestamp() > self.trade_start_time.timestamp()

    def get_mails(self):
        payloads = []
        try:
            # GETS LIST OF ALL EMAILS
            labels = self.service.users().labels().list(userId='me').execute()
            labelLst = labels['labels']
            print(labelLst)
            tos_alerts_label = [l for l in labelLst if 'name' in l and l['name'] == 'tos_alerts'][0]
            results = self.service.users().messages().list(userId='me', labelIds=[tos_alerts_label['id']], q=f'from:{self.alert_sender} is:unread').execute()
            #results = self.service.users().messages().list(userId='me', q=f'from:{self.alert_sender} is:unread').execute()

            if results['resultSizeEstimate'] != 0:

                # {'id': '173da9a232284f0f', 'threadId': '173da9a232284f0f'}
                msg_ids = []
                for message in results["messages"]:
                    msg_id = message["id"]
                    self.logger.info(f'Processing email msg : {msg_id}')
                    if msg_id not in self.__processed_msg_ids:
                        result = self.service.users().messages().get(
                            id=message["id"], userId="me", format="full", ).execute()
                        content = result['snippet']
                        sender = None
                        subject = None
                        for payload in result['payload']["headers"]:
                            if payload["name"] == "From":
                                sender = payload["value"]
                            if payload["name"] == "Subject":
                                subject = payload["value"]
                            if payload["name"] == "Date":
                                ts = payload["value"]
                        payloads.append({
                            "sender": sender,
                            "subject": subject,
                            "content": content,
                            "ts": ts
                        })
                        print({
                            "sender": sender,
                            "subject": subject,
                            "content": content,
                            "ts": ts
                        })
                        self.__processed_msg_ids.add(msg_id)
                        # MOVE EMAIL TO TRASH FOLDER
                        self.service.users().messages().modify(
                            userId='me', id=message["id"], body={'removeLabelIds': ['UNREAD']}).execute()
                        self.service.users().messages().trash(
                            userId='me', id=message["id"]).execute()
                    else:
                        self.logger.info(f'message processed already. ignored. id = {msg_id}')

        except Exception as e:
            self.logger.exception()
        finally:
            return self.__process_mails(payloads)


if __name__ == "__main__":
    queue = Queue()
    gmail = GmailReader(config=None, event_q=queue)
    gmail.start()
