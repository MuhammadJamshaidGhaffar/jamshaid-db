import aiofiles
import uuid , os , json

########## My Resources #######################
from utility_functions import *

###################### GLOBAL VARIABLES #################
basePath = "./Data/"
on_add_parameters = ['collection' , 'data']


#########################################################
class Get_Document:
    async def on_post(self, req, res):
        on_get_delete_parameters = {"collection": False, "_id": False, "range": False, "filters": False}
        # convert body json into python dictionary
        body = await req.get_media()
        if "range" not in body:
            body["range"] = [0,-1]
        # check if body satisfies the parameters
        for key in on_get_delete_parameters.keys():
            if key in body:
                on_get_delete_parameters[key] = True
        if not (on_get_delete_parameters["collection"]  and (
                on_get_delete_parameters['_id'] or (
                on_get_delete_parameters['range'] and on_get_delete_parameters['filters']))):
            res.status = 404
            res.body = json.dumps({"msg:":"Invalid Parameters"})
            print("/document --- Put Request made with invalid parameters")
            return
        # 1 : if id is present
        if on_get_delete_parameters['_id']:
            path = f"{basePath}/{body['collection']}/{body['_id']}.json"
            try:
                file = await aiofiles.open(path, 'r')
                document = await file.read()
                await file.close()
                res.status = 200
                res.body = document
                return
            except Exception as error:
                print(error)
                res.status = 499
                res.body = json.dumps({"msg:": "Error in reading the file"})
                return
        # 2 : if id is not present
        elif on_get_delete_parameters['range'] and on_get_delete_parameters['filters']:
            print("1 completed")
            path = f"{basePath}{body['collection']}/"
            documents_ids = os.listdir(path)
            print("2 completed")
            filters = body['filters']

            filtered_documents = []
            # looping through all the documents
            for document_id in documents_ids:
                try:
                    print(document_id)
                    file = await aiofiles.open(f"{path}{document_id}", "r")
                    print("3 completed")
                    document = json.loads(await file.read())
                    await file.close()
                    matched = True

                    # matching the document againts the filters
                    if not is_document_match_filters(document , filters) :
                        continue
                    print("Document Matched : " , document)
                    # if document matched against filters
                    filtered_documents.append(document)
                except Exception as error:
                    print(f"Failed I/o in file {path}{document_id}")
            #now all documents have read and filtered
            #now it's time to slice the filtered list for specific range
            print("All Matched Documents are :" , filtered_documents)
            start = body['range'][0]
            end = body['range'][1]
            filtered_documents_length = len(filtered_documents)
            if end > filtered_documents_length or end == -1 :
                end = filtered_documents_length
            sliced_filtered_documents = filtered_documents[start:end]
            response = {}
            response["documents"] = sliced_filtered_documents
            response["remaining"] =  filtered_documents_length- end
            json_response = json.dumps(response)
            res.status = 200
            res.body = json.dumps(json_response)
            return




class Document:
    async def on_post(self , req,res):
        print("Post request on /docment to create document")
        on_get_delete_parameters = {"collection": False, "_id": False, "range": False, "filters": False}
        #convert body json into python dictionary
        body = await req.get_media()
        if "range" not in body:
            body["range"] = [0,-1]
        # check if body satifies the parameters
        for key in on_add_parameters:
            if key not in body.keys():
                res.status = 404
                res.text = json.dumps({"msg":"Invalid Parameters"})
                return
        # create directory if not exists
        path = basePath+ body['collection']
        if not os.path.exists(path):
            os.makedirs(path)
        id = str(uuid.uuid4())
        path = path + f"/{id}.json"
        #open file in write binary mode
        try:
            document = body['data']
            document['_id'] = id
            file = await aiofiles.open(path , 'w')
            await file.write(json.dumps(document))
            await file.close()
            res.status = 200
            res.text = json.dumps({"msg":"Document Created successfully"})
            return
        except Exception as error:
            res.status = 500
            res.text = json.dumps({"msg":"Failed to open the file"})
            print(error)
            return
    async def on_put(self , req, res):
        on_get_delete_parameters = {"collection": False, "_id": False, "range": False, "filters": False}
        #convert body json into python dictionary
        body = await req.get_media()
        if "range" not in body:
            body["range"] = [0,-1]
        #check if body satisfies the parameters
        on_get_delete_parameters['update'] = False
        for key in on_get_delete_parameters.keys():
            if key in body:
                on_get_delete_parameters[key] = True
        if not(on_get_delete_parameters["collection"] and on_get_delete_parameters['update'] and (on_get_delete_parameters['_id'] or (on_get_delete_parameters['range'] and on_get_delete_parameters['filters']))):
            res.status = 404
            res.text = json.dumps({"msg":"Invalid Parameter" , "query":on_get_delete_parameters})
            print("/document --- Put Request made with invalid parameters")
            return
        # 1 : if id is present
        if on_get_delete_parameters['_id']:
            path = f"{basePath}/{body['collection']}/{body['_id']}.json"
            try:
                file = await aiofiles.open(path , 'r')
                document = json.loads(await file.read())
                await file.close()
                #update the document
                document = updateDocument(document , body['update'])

                json_document = json.dumps(document)
                file = await aiofiles.open(path, 'w')
                await file.write(json_document)
                await file.close()
                res.status = 200
                res.text = json_document
                return
            except Exception as error:
                print(error)
                res.status = 499
                res.text = json.dumps({"msg":error})
                return
        # 2 : if id is not present
        elif on_get_delete_parameters['range'] and on_get_delete_parameters['filters'] :
            print("1 completed")
            path = f"{basePath}/{body['collection']}/"
            documents_ids = os.listdir(path)
            print("2 completed")
            filters = body['filters']

            filtered_documents = []
            #looping through all the documents
            for document_id in documents_ids:
                try:
                    print(document_id)
                    file = await aiofiles.open(f"{path}{document_id}" , "r")
                    print("3 completed")
                    document = json.loads( await file.read())
                    await file.close()
                    matched = True

                    # matching the document againts the filters
                    if not is_document_match_filters(document , filters):
                        continue
                    filtered_documents.append(document)
                except Exception as error:
                    print(f"Failed I/o in file {path}{document_id}")
            #now write the documents which falls in range
            start = body['range'][0]
            end = body['range'][1]
            len_filtered_documents = len(filtered_documents)
            if end > len_filtered_documents or end == -1 :
                end = len_filtered_documents
            for i in range(start , end):
                document = filtered_documents[i]
                try:
                    # update document object
                    document = updateDocument(document , body['update'])
                    filtered_documents[i] = document
                except Exception as error:
                    print("Error in updating document : ", error)
                    continue
                # for key , value in body['update'].items():
                #     document[key] = value
                json_document = json.dumps(document)
                try:
                    file = await aiofiles.open(f"{path}{document['_id']}.json" , "w")
                    await file.write(json_document)
                    await file.close()
                except Exception as err:
                    print("Error while writing to file! " , err)

            # slice the doucments
            sliced_filtered_documents = filtered_documents[start:end]
            response = {}
            response["documents"] = sliced_filtered_documents
            response["remaining"] = len_filtered_documents - end
            json_response = json.dumps(response)
            res.status = 200
            res.text = json.dumps(json_response)
            return
    async def on_delete(self , req, res):
        on_get_delete_parameters = {"collection": False, "_id": False, "range": False, "filters": False}
        #convert body json into python dictionary
        body = await req.get_media()
        if "range" not in body:
            body["range"] = [0,-1]
        #check if body satisfies the parameters
        for key in on_get_delete_parameters.keys():
            if key in body:
                on_get_delete_parameters[key] = True
        if not(on_get_delete_parameters["collection"]  and (on_get_delete_parameters['_id'] or (on_get_delete_parameters['range'] and on_get_delete_parameters['filters']))):
            res.status = 404
            res.text = json.dumps({"msg":"Invalid Parameters"})
            print("/document --- Put Request made with invalid parameters")
            return
        # 1 : if id is present
        if on_get_delete_parameters['_id']:
            path = f"{basePath}{body['collection']}/{body['_id']}.json"
            response = await delete_document(path)
            if response["is_deleted"]:
                res.status = 200
            else:
                res.status = 404
            res.text = json.dumps({"msg":response["msg"]})
            return
        # 2 : if id is not present
        elif on_get_delete_parameters['range'] and on_get_delete_parameters['filters'] :
            print("1 completed")
            path = f"{basePath}{body['collection']}/"
            documents_ids = os.listdir(path)
            print("2 completed")
            filters = body['filters']

            filtered_documents = []
            #looping through all the documents
            for document_id in documents_ids:
                try:
                    print(document_id)
                    file = await aiofiles.open(f"{path}{document_id}" , "r")
                    print("3 completed")
                    document = json.loads( await file.read())
                    await file.close()
                    matched = True

                    # matching the document againts the filters
                    if not is_document_match_filters(document , filters):
                        continue
                    filtered_documents.append(document)

                except Exception as error:
                    print(f"Failed in reading the document {path}{document_id}")
            #now delete the documents which falls in range
            print("Deleting the documents")
            start = body['range'][0]
            end = body['range'][1]
            len_filtered_documents = len(filtered_documents)
            if end > len_filtered_documents or end == -1 :
                end = len_filtered_documents
            deleted_documents = []
            print("filtered documents : "  , filtered_documents)
            for i in range(start , end):
                document = filtered_documents[i]
                path = f"{basePath}{body['collection']}/{document['_id']}.json"

                print("deleting the document " , path)
                # delete document object
                response = None
                try:
                    response = await delete_document(path)
                except Exception as error:
                    print(error)
                response["document"] = document
                deleted_documents.append(response)
            sliced_filtered_documents = filtered_documents[start:end]
            response = {}
            response["documents"] = sliced_filtered_documents
            response["remaining"] = len_filtered_documents - end
            json_response = json.dumps(response)
            res.status = 200
            res.body = json.dumps(json_response)
            return







