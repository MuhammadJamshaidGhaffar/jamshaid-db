import falcon.asgi
from methods import *

app = falcon.asgi.App(middleware=falcon.CORSMiddleware(
    allow_origins='*'))

app.add_route("/document" , Document())
app.add_route("/get_document" , Get_Document())