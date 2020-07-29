import csv
import re
import pymongo
from pymongo import MongoClient
import datetime


class Ticket:

    def __init__(self):
        client = MongoClient()
        self.db = client['db_tickets']

    def read_data(self, file):
        # Загрузить данные в бд из CSV-файла
        with open(file, encoding='utf8') as csv_file:
            # прочитать файл с данными и записать в коллекцию
            reader = csv.DictReader(csv_file)
            for item in reader:
                add_item = dict()
                for key in item:
                    if key == 'Цена':
                        add_item[key] = int(item[key])
                    elif key == 'Дата':
                        date_split = item[key].replace('0', '').split('.')
                        add_item[key] = datetime.datetime(2019, int(date_split[1]), int(date_split[0]))
                    else:
                        add_item[key] = item[key]
                self.db.tickets.insert_one(add_item)

    def find_cheapest(self):
        """
        Отсортировать билеты из базы по возрастания цены
        Документация: https://docs.mongodb.com/manual/reference/method/cursor.sort/
        """
        for record in self.db.tickets.find().sort('Цена', pymongo.ASCENDING):
            print(record)

    def find_by_name(self, name):
        """
        Найти билеты по имени исполнителя (в том числе – по подстроке),
        и вернуть их по возрастанию цены
        """
        regex = re.compile(re.escape(name), re.I)
        for record in self.db.tickets.find({'Исполнитель': regex}).sort('Цена', pymongo.ASCENDING):
            print(record)


if __name__ == '__main__':
    mongodbTickets = Ticket()
    mongodbTickets.read_data('artists.csv')
    print('==================================================')
    mongodbTickets.find_cheapest()
    print('==================================================')
    mongodbTickets.find_by_name('Enter')
