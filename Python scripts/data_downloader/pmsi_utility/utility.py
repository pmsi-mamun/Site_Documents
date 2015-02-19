__author__ = 'atalhan'
import os, re, csv, logging, datetime
import logging.handlers

class helper:

    @staticmethod
    def write_data_file(data, file, field_list, add_headers):
        fields = ""
        for f in field_list:
            fields = fields + f + ","

        with open(file, "a", encoding="utf-8") as myfile:
            if add_headers:
                myfile.write(fields + "\n")

            counter = 0

            for d in data:
                try:
                    counter += 1
                    sb = ""
                    new_id = ''
                    for f in field_list:
                        try:
                            r_val = d['_source'][f]

                            if r_val is None:
                                continue

                            val = str(r_val).strip().replace(",", "")
                            sb += val + ","

                        except Exception as e:
                            sb += ","

                    sb += "\n"
                    myfile.write(sb.encode('utf-8').decode('utf-8'))
                except Exception as e:
                    print(e)

    @staticmethod
    def get_valid_filename(folder, filename, counter, extension="csv"):
        path = "{folder}{filename} - {counter}.{extension}".format(folder=folder, filename=filename, counter=counter,
                                                                   extension=extension)
        if os.path.isfile(path):
            return helper.get_valid_filename(folder, filename, counter+1, extension)
        else:
            return path

    @staticmethod
    def remove_foreign_chars(sb):
        try:
            m = re.search('[^a-zA-Z0-9\s\-_,]', sb)
            if m is None:
                return sb

            sb = sb.replace(m.group(0), "")
            return helper.remove_foreign_chars(sb)
        except Exception as e:
            print(e)

    @staticmethod
    def remove_foreign_chars_pattern(sb, pattern):
        try:
            m = re.search(pattern, sb)
            if m is None:
                return sb

            sb = sb.replace(m.group(0), "")
            return helper.remove_foreign_chars(sb)
        except Exception as e:
            print(e)

    @staticmethod
    def get_file_data(csv_file_name, fields):
        csvfile = open(csv_file_name, 'r')
        reader = csv.DictReader(csvfile, fields)
        data = []
        for row in reader:
            data.append(row)

        return data

    @staticmethod
    def get_file_data_rows(file_name):
        file = open(file_name, 'r')

        return file.readlines()

    @staticmethod
    def setup_logger(folder, filename, logger, add_console=True, log_level=logging.INFO):
        ch = logging.StreamHandler()
        file_name = helper.get_valid_filename(folder, filename, 0, "log")
        hdlr = logging.handlers.RotatingFileHandler(file_name, maxBytes=10)
        formatter = logging.Formatter('%(asctime)s %(levelname)s - %(message)s ')
        hdlr.setFormatter(formatter)
        ch.setFormatter(formatter)
        logger.addHandler(hdlr)
        if add_console:
            logger.addHandler(ch)

        logger.setLevel(log_level)

    @staticmethod
    def remove_foreign_chars(sb):
        try:
            m = re.search('[^a-zA-Z0-9\s\-]', sb)
            if m is None:
                return sb

            sb = sb.replace(m.group(0), "")
            return helper.remove_foreign_chars(sb)
        except Exception as e:
            print(e)

    @staticmethod
    def write_data_file(data, file, field_list):
        fields = ""
        for f in field_list:
            fields = fields + f + ","

        with open(file, "a", encoding="utf-8") as myfile:
            myfile.write(fields + "\n")
            counter = 0

            for d in data:
                try:
                    counter += 1
                    sb = ""
                    new_id = ''
                    for f in field_list:
                        try:
                            r_val = d[f]

                            if r_val is None:
                                continue

                            val = str(r_val).strip().replace(",", "").replace(" ", "_")
                            sb += val + ","

                            if f == 'countrycode':
                                new_id += '_' + val
                            elif f == 'streetname' or f == 'zipcode' or f == 'town':
                                new_id += val
                        except Exception as e:
                            sb += ","

                    sb += ',' + new_id
                    sb = helper.remove_foreign_chars(sb) + "\n"
                    ###sb += "\n"
                    myfile.write(sb.encode('utf-8').decode('utf-8'))
                except Exception as e:
                    print(e)

    def get_file_headers(file_name):
        file = open(file_name, 'r')

        return file.readline()