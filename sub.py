import time
from concurrent.futures import TimeoutError

from google.api_core.exceptions import NotFound
from google.cloud import pubsub_v1
import sys

project_id = "sac-exam-22"
topic_id = "cantieri"
subscription_id = "cantieri_sub"

subscriber = pubsub_v1.SubscriberClient()
subscription_path: str = subscriber.subscription_path(project_id, subscription_id)

publisher = pubsub_v1.PublisherClient()
topic_path: str = publisher.topic_path(project_id, topic_id)

# Lista dei CAP, separati con virgola
caplist = None
if len(sys.argv) == 2:
    caplist = list(sys.argv[1].split(','))


def callback(message) -> None:
    """
    La funzione da chiamare quando si riceve un nuovo cantiere
    :param message: Il nuovo cantiere
    """
    msg: str = message.data.decode()
    new_cap: str = message.attributes['cap']

    print(f"Nuovo cantiere: {msg}, {new_cap}.")

    message.ack()


# Creiamo la lista dei filtri, in base agli eventuali CAP passati
filt = "attributes:cap"
if caplist is not None:
    filt += " AND ("
    for cap in caplist:
        filt += f'attributes.cap = "{str(cap)}" OR '
    filt = filt[:-4] + ")"

with subscriber:
    try:
        subscriber.delete_subscription(request={"subscription": subscription_path})
    except NotFound:
        pass

    # Possono verificarsi errori se non si aspetta un attimo
    time.sleep(5)

    subscription = subscriber.create_subscription(
        request={"name": subscription_path, "topic": topic_path, "filter": filt}
    )

    time.sleep(5)

    streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)

    try:
        print("Aspetto nuovi cantieri...")
        streaming_pull_future.result()
    except TimeoutError:
        streaming_pull_future.cancel()
        streaming_pull_future.result()
