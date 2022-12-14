import aiofiles.os
import os , copy


def is_document_match_filters(document , filters):

    try:
        matched = True
        # matching the document againts the filters
        for key, value in filters.items():
            if key == "$or" and isinstance(value , list):
                # Or operator , it is in document space so it does not mean it is nested, it checks for props in document space
                matched = False
                for valueItems in value:
                    if is_document_match_filters(document , valueItems):
                        matched = True
                        break
                if not matched:
                    break
                continue
            if key == "$and" and isinstance(value , list):
                # and operator , it is in document space so it does not mean it is nested, it checks for props in document space
                matched = True
                for valueItems in value:
                    if not is_document_match_filters(document , valueItems):
                        matched = False
                        break
                if not matched:
                    break
                continue
            # checking if current value is list
            # if it is not list
            if not isinstance(value, list):
                if key not in document or value != document[key]:
                    matched = False
                    break
            else: # if it is list
                # it is checking if first element of list is string and it is also an operator
                if isinstance(value[0] , str) and value[0][0] == "$"  :
                    operator = value[0]
                    values = value[1]
                    if operator == "$filters" and isinstance(values , dict):
                        print("\n\nHere\n\n\n")
                        print("\n")
                        print("Orginal document = " , document)
                        print("Document = ", document[key])
                        print("filters = ", values)
                        print("\n")
                        if not is_document_match_filters( document[key] , values):
                            print("\nDocument not matched")
                            matched = False
                            break
                        print("matched")
                        continue
                    matched = False
                    # set matched = false and loop through all values in list
                    # if not matched after looping though all values in list
                    # then break the outer loop

                    for valueItem in values:
                        if operator == "$eq":
                            if valueItem == document[key]:
                                matched = True
                                break
                        elif operator == "$in" and isinstance(document[key], list):
                            print("checking")
                            if valueItem in document[key]:
                                matched = True
                                break
                        elif operator == "$substring" and isinstance(document[key], str):
                            if valueItem in document[key]:
                                matched = True
                                break
                        elif operator == "$substring/i" and isinstance(document[key], str):
                            if valueItem.lower() in document[key].lower():
                                matched = True
                                break
                        elif isinstance(document[key], int) or isinstance(document[key], float):
                            if operator == "$lt":
                                if document[key] < valueItem:
                                    matched = True
                                    break
                            elif operator == "$lte":
                                if document[key] <= valueItem:
                                    matched = True
                                    break
                            elif operator == "$gt":
                                if document[key] > valueItem:
                                    matched = True
                                    break
                            elif operator == "$gte":
                                if document[key] >= valueItem:
                                    matched = True
                                    break
                    # if after looping though all elements in list does not matched
                    # then break the loop
                    if not matched:
                        break
                # now first element in list is either not string or if string but not has $ => it is not operator
                else:
                    if value != document[key]:
                        matched = False
                        break

    except Exception as error:
        print(f"Error while matching document with filters.Maybe prop is not present in document" )
        print(error)
        matched = False
    return matched

async def delete_document(path):
    msg = ""
    is_deleted = False
    if not os.path.exists(path):
        msg = "The document does not exists"
        print(msg)
        print(path)
    else:
        try:
            await aiofiles.os.remove(path)
            msg = "Document deleted successfully"
            is_deleted = True
            print(msg)
            print(path)
        except Exception as error:
            msg = "Failed to delete the document " + error
            is_deleted = False
            print(msg)
            print(path)
    return {"is_deleted":is_deleted , "msg":msg}

def updateDocument(document , update):
    copiedDocument = copy.copy(document)
    for key , value in update.items():
        if isinstance(value,list) and len(value) != 0 and isinstance(value[0] , str) and len(value[0]) !=0 and value[0][0] == "$":

            if len(value) == 1:
                raise Exception(F"A list of values on which ${value[0]} is to be performed must be present")
            if (value[0] == "$push" or value[0] == "$append" ) and isinstance(value[1] , list):
                if key not in copiedDocument:
                    copiedDocument[key] = value[1]
                    continue
                for items in value[1]:
                    copiedDocument[key].append(items)
            elif value[0] == "$remove" and isinstance(value[1] , list):
                if key not in copiedDocument:
                    continue
                if type(document[key]) != list:
                    raise Exception(f"Document[key] = {document[key]} is not a list")
                for item in value[1]:
                    if item in document[key]:
                        copiedDocument[key].remove(item)
            elif value[0] == "$insert" and isinstance(value[1] , int) and isinstance(value[2] , list):
                if key not in copiedDocument:
                    copiedDocument[key] = value[2]
                    continue
                for items in reversed(value[2]):
                    copiedDocument[key].insert(value[1] , items)
            elif value[0] == "$update_obj" and isinstance(value[1] , dict) :
                if key not in copiedDocument:
                    copiedDocument[key] = value[1]
                    continue
                copiedDocument[key] = updateDocument(document[key] , value[1])

        else:
            copiedDocument[key] = value
    return copiedDocument

def outputDocument(document , output_filters):
    if output_filters == "*":
        return document
    else:
        output_document = {}
        for key, value in output_filters.items():
            #if key doesn't exist in document
            if key not in document:
                print("Error !" ,document , key ,  value)
                raise Exception(f"{key} doesn't exist in document")
            # if value is dict then go for nested loop
            if isinstance(value , dict) and isinstance(document[key] , dict):
                output_document[key] = outputDocument(document[key] , value)
            # but if value is 1 or * then return whole value no matter what type of value is
            elif value == 1 or value == "*":
                output_document[key] = document[key]
            else:
                raise Exception(f"Invalid value for {key}")

        return output_document
