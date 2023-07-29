import hashlib

class MetaData:
    def __init__(self):
        self.data = [] # folder which data will be stored {[hash: ..., ip: .., port: ..]}
        self.ranges =[] # ranges of keys {[start: ..., end: ..., ip: .., port: ..]}

    def add_to_hex(self, hex_string):
        integer_value = int(hex_string, 16)  # Convert hex string to integer
        incremented_value = integer_value + 1  # Increment the integer by 1
        result_hex_string = hex(incremented_value)[2:].upper() 
        return result_hex_string

    def sort_meta(self):
        self.data.sort(key=lambda x: x['hash'])

    def compute_md5_hash(self, string):
        md5_hash = hashlib.md5()
        md5_hash.update(string.encode('utf-8'))
        return md5_hash.hexdigest()

    def add_server(self, ip, port):
        self.data.append({'hash': self.compute_md5_hash(ip+port), 'ip' : ip, 'port': port})
        self.sort_meta()
        self.compute_ranges()
        return self.data
    
    def remove_server(self, ip, port):
        #TODO remove server from meta data
        # logging.error(self.data)
        # logging.error({'hash': self.compute_md5_hash(ip+port), 'ip' : ip, 'port': port})
        self.data.remove({'hash': self.compute_md5_hash(ip+port), 'ip' : ip, 'port': port})
        self.sort_meta()
        self.compute_ranges()
        return self.data

    def compute_ranges(self):
        ranges=[]
        last='0'*32
        count=1
        for index, i in enumerate(self.data):
            r={}
            # logging.error('range ' + str(count))

            if index==0 and len(self.data)>1:
                # logging.error('start ' + add_to_hex(meta[-1]['hash']))
                r['start']=self.add_to_hex(self.data[-1]['hash'])
            elif index==0 and len(self.data)==1:
                r['start']='0'*32
                r['end']='F'*32
            else:
                # logging.error('start ' + add_to_hex(last))
                r['start']=self.add_to_hex(last)
            for k,v in i.items():
                # if index == len(meta) - 1 and k == 'hash':
                #     continue
                if k == 'hash':
                    # logging.error('end' + ' ' + v)
                    if len(self.data)==1:
                        pass
                    else:
                        r['end']=v
                else:
                    # logging.error(k+ ' ' + v)
                    r[k]=v
            # logging.error('\n')
            # count+=1
            ranges.append(r)
            last=i['hash']
        self.ranges=ranges
        # for k,v in meta[-1].items():
        #     logging.error(last,'FFFFF',v[0], v[1])

    # # Example usage
    # ip='192.168.1.1'
    # port='5453'
    # add_server(meta_data,ip, port)

    # # ip2='192.168.1.4'
    # # port2='5457'
    # # add_server(meta_data,ip2, port2)

    # logging.error(compute_ranges(sort_meta(meta_data)))

    # ip3='192.168.1.9'
    # port3='5458'
    # add_server(meta_data,ip3, port3)

    # # logging.error(sort_meta(meta_data))

    # # logging.error(meta_data)
    # logging.error(compute_ranges(sort_meta(meta_data)))
