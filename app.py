from flask import Flask, request, render_template
from flask_restful import Api, Resource
from google.cloud import firestore, pubsub_v1
from google.cloud.firestore_v1 import CollectionReference, DocumentSnapshot
from marshmallow import fields, Schema
from wtforms import Form, BooleanField, validators
from wtforms.fields.numeric import IntegerField

client = firestore.Client()

app = Flask(__name__,
            static_folder='static',
            static_url_path='/static')

api = Api(app=app, prefix="/api/v1/")


class QueryForm(Form):
    """
    Il form per la pagina web
    """
    cap = IntegerField("ip", [validators.DataRequired(), validators.number_range(min=10000, max=99999)])
    cantieri = BooleanField("cantieri")
    umarell = BooleanField("umarell")


def db(collection: str) -> CollectionReference:
    """
    Semplice funzione per accorciare il codice
    :param collection: La collezione di firestore
    :return: La collezione di firestore
    """
    return client.collection(collection)


class CantiereSchema(Schema):
    """
    Lo schema che descrive un cantiere, composto da indirizzo e CAP.
    """
    indirizzo = fields.Str(required=True)
    cap = fields.Int(required=True, min=10000, max=99999)


class UmarellSchema(Schema):
    """
    Lo schema che descrive un umarell, composto da nome, cognome e CAP.
    """
    nome = fields.Str(required=True)
    cognome = fields.Str(required=True)
    cap = fields.Int(required=True, min=10000, max=99999)


class IdSchema(Schema):
    """
    Semplice schema per validare gli id
    """
    id = fields.Int(min=1)


class Umarell(Resource):
    """
    API umarell
    """
    def get(self, uid: str):

        if IdSchema().validate({"id": uid}):
            return None, 400

        rule: DocumentSnapshot = db("umarell").document(uid).get()

        if not rule.exists:
            return None, 404

        return rule.to_dict()

    def post(self, uid: str):

        if IdSchema().validate({"id": uid}):
            return None, 400

        reqjson = request.get_json()

        if UmarellSchema().validate(reqjson):
            return None, 400

        rule = db("umarell").document(uid).get()

        if rule.exists:
            return None, 409

        db("umarell").document(uid).set(reqjson)

        return reqjson, 201


class Cantiere(Resource):
    """
    API cantiere
    """
    def get(self, cid: str):

        if IdSchema().validate({"id": cid}):
            return None, 400

        rule = db("cantiere").document(cid).get()

        if not rule.exists:
            return None, 404

        return rule.to_dict()

    def post(self, cid: str):

        if IdSchema().validate({"id": cid}):
            return None, 400

        reqjson = request.get_json()

        if CantiereSchema().validate(reqjson):
            return None, 400

        rule = db("cantiere").document(cid).get()

        if rule.exists:
            return None, 409

        db("cantiere").document(cid).set(request.get_json())

        # Effettua il publishing su PubSub
        publisher = pubsub_v1.PublisherClient()
        topic_name = 'projects/{project_id}/topics/{topic}'.format(
            project_id="sac-exam-22",
            topic='cantieri')
        future = publisher.publish(topic_name,
                                   f"{request.get_json()['indirizzo']}".encode(),
                                   cap=str(request.get_json()['cap']))
        future.result()

        return request.get_json(), 201


class Clean(Resource):
    """
    Cancella tutto il database
    """
    def get(self):
        for doc in db("umarell").list_documents():
            doc.delete()
        for doc in db("cantiere").list_documents():
            doc.delete()
        return None, 200


# Aggiunge le API all'applicazione
api.add_resource(Clean, "clean")
api.add_resource(Umarell, "umarell/<uid>")
api.add_resource(Cantiere, "cantiere/<cid>")


@app.route('/', methods=['GET', 'POST'])
def root():
    """
    Index page
    """

    # Il form di ricerca per CAP
    form = QueryForm(request.form)

    # Lista dei risultati
    results = []

    if request.method == "POST":

        if not form.validate():
            return "Errore di validazione", 400

        # Se la checkbox umarell è spuntata
        if form.umarell.data:
            # Cerchiamo tutti gli umarell con quel CAP
            for f in db("umarell").where('cap', '==', form.cap.data).stream():
                # E aggiungiamo "Nome Cognome" alla lista dei risultati
                results.append(f"{f.to_dict()['nome']} {f.to_dict()['cognome']}")

        if form.cantieri.data:
            for f in db("cantiere").where('cap', '==', form.cap.data).stream():
                results.append(f"{f.to_dict()['indirizzo']}")

    return render_template("index.html", results=results, form=form), 200


if __name__ == '__main__':
    app.run()
