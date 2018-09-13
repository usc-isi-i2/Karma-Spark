__author__ = 'dipsy'

import requests
import json
import re
from os import listdir
from os.path import isfile, join, exists

'''
Class to load model information from Github.
From a public Github repo, if you point it to a base models folder, it assumes that the name of the models is the
folder name, and inside that folder all models for that source exist with the naming convention *-model.ttl.
The root for the corresponding model is assumed to be in a file named *-root.txt
For example if you have a models folder:

-models
---|-google
---|----|-google-model.ttl
---|----|-google-root.txt
---|-yahoo
---|----|-yahoo1-model.ttl
---|----|-yahoo1-root.txt
---|----|-yahoo2-model.ttl
---|----|-yahoo2-root.txt
---|----|-yahoo2-model.MD
---|----|-x.png

It will extract the model information and return an array as follows:
[
{"name":"google", "url":"absolute url of google-model.ttl", "root":"contents of google-root.txt file"},
{"name":"yahoo", "url":"absolute url of yahoo1-model.ttl", "root":"contents of yahoo1-root.txt file"},
{"name":"yahoo", "url":"absolute url of yahoo2-model.ttl", "root":"contents of yahoo2-root.txt file"},
]

See the main method for usage
'''

class GitModelLoader:

    def __init__(self, organization, repository_name, branch, local_folder=None):
        self.base_url = "https://api.github.com/repos/" + organization + "/" + repository_name + "/contents/"
        self.raw_url = "https://raw.githubusercontent.com/" + organization + "/" + repository_name + "/" + branch
        self.branch = branch
        self.local_folder = local_folder

    def get_models_from_folder(self, folder_path):
        if self.local_folder is not None:
            return self.get_models_from_directory(folder_path)
        else:
            return self.get_models_from_github_folder(folder_path)

    def get_models_from_github_folder(self, folder_path):
        models_folder_json_arr = self.__query_github(folder_path)
        models = []
        if models_folder_json_arr is not None:
            for models_folder_json in models_folder_json_arr:
                model_name = models_folder_json["name"]
                if models_folder_json["type"] == "dir":
                    models_inner_folder_json = self.__query_github(models_folder_json["path"])

                    #First get all the models
                    model_uris = {}
                    for model_file_details in models_inner_folder_json:
                        file_name = model_file_details['name']
                        if re.search(r'.*-model\.ttl$', file_name):
                            model_uris[file_name] = model_file_details["download_url"]

                    #Now get all roots for the models
                    for model_file_name in model_uris:
                        root_file_name = model_file_name[0: len(model_file_name)-10] + "-root.txt"
                        #print "FInd:", root_file_name

                        #Now find the root file and load it
                        root = None
                        for model_file_details in models_inner_folder_json:
                            if model_file_details["name"] == root_file_name:
                                root = self.__get_request(model_file_details["download_url"])
                                break

                        if root is not None:
                            root = root.strip()
                            models.append({"name": model_name, "url": model_uris[model_file_name], "root": root})

        return models

    def get_models_from_directory(self, folder_path):
        models = []
        folders = [f for f in listdir(self.local_folder + "/" + folder_path) if not isfile(join(self.base_url + "/" + folder_path, f))]
        for folder in folders:
            folder_name = self.local_folder + "/" + folder_path + "/" + folder
            if isfile(folder_name):
                continue

            model_name = folder
            folder_files = [f for f in listdir(folder_name) if isfile(join(folder_name, f))]

            #First get all the models
            model_uris = {}
            for f in folder_files:
                if re.search(r'.*-model\.ttl$', f):
                   model_uris[f] = self.raw_url + "/" + folder_path + "/" + model_name + "/" + f

            for model_file_name in model_uris:
                root_file_name = model_file_name[0: len(model_file_name)-10] + "-root.txt"
                root = self.__read_file(folder_name + "/" + root_file_name)

                if root is not None:
                    root = root.strip()
                    models.append({"name": model_name, "url": model_uris[model_file_name], "root": root})
        return models

    def __query_github(self, folder_path):
        url = self.base_url + folder_path + "?ref=" + self.branch
        print(url)
        response = self.__get_request(url)
        if response is not None:
            return json.loads(response)
        return None

    def __get_request(self, url):
        response = requests.get(url, verify=False, timeout=10*60)
        if response.status_code == requests.codes.ok:
            return str(response.content)
        return None


    def __read_file(self, filename):
        content = None
        if exists(filename):
            with open(filename, 'r') as content_file:
                content = content_file.read()

        return content

if __name__ == "__main__":
    gitModelLoader = GitModelLoader("usc-isi-i2", "effect-alignment", "master", "/Users/dipsy/github-effect/effect/effect-alignment")
    models = gitModelLoader.get_models_from_folder("models")
    print (json.dumps(models))