__author__ = 'dipsy'

import requests
import json
import re
from os import listdir
from os.path import isfile, join, exists

'''
Class to load all frames from Github.
From a public Github repo, if you point it to a frames folder, it will read all .json files as frames.
It reads the frames and parses it to get all the types that the framer would need.

See the main method for usage
'''


class GitFrameLoader:
    def __init__(self, organization, repository_name, branch, local_folder=None):
        self.base_url = "https://api.github.com/repos/" + organization + "/" + repository_name + "/contents/"
        self.branch = branch
        self.types = {}
        self.context = {}
        self.raw_url = "https://raw.githubusercontent.com/" + organization + "/" + repository_name + "/" + branch
        self.local_folder = local_folder

    def get_frames_from_folder(self, folder_path, local=False):
        if self.local_folder is not None:
            return self.get_frames_from_directory(folder_path, local)
        else:
            return self.get_frames_from_github(folder_path)

    def get_frames_from_directory(self, folder_path, local=False):
        frames = []
        folder_full_path = self.local_folder + "/" + folder_path
        files = [f for f in listdir(folder_full_path) if f.endswith(".json") and isfile(join(folder_full_path, f))]
        for file_name in files:
            frame_name = file_name[0:len(file_name) - 5]
            if local is True:
                frame = {"name": frame_name, "url": folder_full_path + "/" + file_name}
                types = self.__get_types_in_frame(json.load(open(folder_full_path + "/" + file_name)))
            else:
                frame = {"name": frame_name, "url": self.raw_url + "/" + folder_path + "/" + file_name}
                frame_contents = self.__read_file(folder_full_path + "/" + file_name)
                types = self.__get_types_in_frame(json.loads(frame_contents))
            frames.append(frame)
            for type in types:
                self.types[type] = True
        return frames

    def get_frames_from_github(self, folder_path):
        folder_json_arr = self.__query_github(folder_path)
        frames = []
        if folder_json_arr is not None:
            for folder in folder_json_arr:
                frame_name = folder["name"]
                if folder["type"] == "file" and frame_name.endswith(".json"):
                    frame_name = frame_name[0:len(frame_name) - 5]
                    frame = {"name": frame_name, "url": folder["download_url"]}
                    frames.append(frame)
                    frame_contents = self.__get_request(folder["download_url"])
                    types = self.__get_types_in_frame(json.loads(frame_contents))
                    for type in types:
                        self.types[type] = True
        return frames

    def load_context(self, context_file):
        if context_file.startswith("file:///"):
             self.load_context_local(context_file)
        else:
             self.context = self.__load_context(json.loads(self.__get_request(context_file)))

    def load_context_local(self, context_file):
        if context_file.startswith("file:///"):
            context_file = context_file[7:]
        context_json = json.load(open(context_file))
        context_dict = {}
        if "@context" in context_json:
            contexts = context_json["@context"]
            for class_name in contexts:
                definition = contexts[class_name]
                # print class_name, definition
                if type(definition) == dict and "@id" in definition:
                    context_dict[class_name] = definition["@id"]

        # print context_dict
        self.context = context_dict

    def get_types_in_all_frames(self, context_file=None):
        type_arr = []
        if context_file is not None:
            self.load_context(context_file)

        for type in self.types:
            url = type
            if self.context is not None:
                url = self.context[type]
            type_arr.append({"name": type, "uri": url})
        return type_arr

    def __get_types_in_frame(self, frame):
        types = {}
        for attr in frame:
            value = frame[attr]
            if attr == "@type":
                types[value] = True
            elif type(value) == dict:
                inner_types = self.__get_types_in_frame(value)
                for t in inner_types:
                    types[t] = True

        return types

    def __load_context(self, context_json):
        context_dict = {}
        if "@context" in context_json:
            contexts = context_json["@context"]
            for class_name in contexts:
                definition = contexts[class_name]
                #print class_name, definition
                if type(definition) == dict and "@id" in definition:
                    context_dict[class_name] = definition["@id"]

        # print context_dict
        return context_dict

    def __query_github(self, folder_path):
        url = self.base_url + folder_path + "?ref=" + self.branch
        print(url)
        response = self.__get_request(url)
        if response is not None:
            return json.loads(response)
        return None

    def __read_file(self, filename):
        content = None
        if exists(filename):
            with open(filename, 'r') as content_file:
                content = content_file.read()

        return content

    def __get_request(self, url):
        response = requests.get(url, verify=False)
        if response.status_code == requests.codes.ok:
            return str(response.content)
        return None


if __name__ == "__main__":
    gitFrameLoader = GitFrameLoader("usc-isi-i2", "effect-alignment", "master", "/Users/dipsy/github-effect/effect/effect-alignment")
    frames = gitFrameLoader.get_frames_from_folder("frames")
    gitFrameLoader.load_context(
        "https://raw.githubusercontent.com/usc-isi-i2/dig-alignment/development/versions/3.0/karma/karma-context.json")
    types = gitFrameLoader.get_types_in_all_frames()
    print (json.dumps(frames))
    print (json.dumps(types))
