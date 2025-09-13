import requests


class APIDataFecher:
    def Fetch(self, url, object_name, next_url_property_name, key):
        response = requests.get(url)
        json_data = response.json()
        data_object_list = []
        for data_object in json_data[object_name]:
            data_object_list.append(data_object)
        while next_url_property_name in json_data:
            print('requesting next page', json_data[next_url_property_name])
            response = requests.get(
                json_data[next_url_property_name]+f'&apiKey={key}')
            json_data = response.json()

            if object_name not in json_data:
                print(f'No {object_name} in response', json_data)
                break
            for data_object in json_data[object_name]:
                data_object_list.append(data_object)
        return data_object_list
