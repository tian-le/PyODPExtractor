from pyrfc import Connection
import json, time

class extractor:
    def __init__(self, src):
        info = json.load(open('odp_%s_config.json' % src, 'r'))
        self.conn = Connection(ashost = info['ashost'], sysnr = info['sysnr'], client = info['client'], user = info['user'], passwd = info['passwd'])
        self.datasource = None
        self.pointer = None
        self.package = None
        self.tt = 0

    def get_details(self, datasource):
        self.datasource = datasource
        self.details = self.conn.call('RODPS_REPL_SOURCE_GET_DETAIL',
                                      I_SUBSCRIBER_TYPE=u'BOBJ_DS',
                                      I_QUEUENAME=datasource)
        if self.details['ET_RETURN']:
            raise Exception('\n'.join([msg['MESSAGE'] for msg in self.details['ET_RETURN']]))
        self.t_fields = self.details['ET_FIELDS']
        return self.details

    def open(self, mode):
        if not self.datasource:
            raise Exception('Call get_details first!')
        self.mode = mode
        self.opener = self.conn.call('RODPS_REPL_SOURCE_OPEN',
                                     I_SUBSCRIBER_TYPE=u'BOBJ_DS',
                                     I_SUBSCRIBER_NAME=u'TEST6',
                                     I_SUBSCRIBER_PROCESS=u'TEST2',
                                     I_QUEUENAME=self.datasource,
                                     I_EXTRACTION_MODE=mode)
        if self.opener['ET_RETURN']:
            raise Exception('\n'.join([msg['MESSAGE'] for msg in self.opener['ET_RETURN']]))
        self.pointer = self.opener['E_POINTER']
        return self.opener

    def fetch(self):
        if not self.pointer:
            raise Exception('Call open first!')
        print('rfc call begin')
        t = time.time()
        ret = self.conn.call('RODPS_REPL_SOURCE_FETCH',
                             I_POINTER=self.pointer,
                             I_MAXPACKAGESIZE=1024*1024*50)
        print('rfc call end')
        print(time.time() - t)
        self.tt += time.time() - t
        if ret['ET_RETURN']:
            raise Exception('\n'.join([msg['MESSAGE'] for msg in ret['ET_RETURN']]))
        self.package = ret['E_PACKAGE']
        data = []
        buf = None
        i = 0
        for t in ret['ET_DATA']:
            if buf == None:
                buf = t['DATA']
            else:
                buf += t['DATA']
            if t['CONTINUATION']:
                continue
            buf = buf.decode('utf-8')
            offset = 0
            i += 1
            row = [self.pointer, self.package, i]
            for col in self.t_fields:
                l = int(col['OUTPUTLENG'])
                val = buf[offset:offset + l]
                if col['TYPE'] in ('CLNT', 'LANG', 'CHAR', 'CUKY', 'UNIT', 'DATS', 'TIMS', 'NUMC'):
                    val = val.rstrip()
                elif col['TYPE'] in ('QUAN', 'CURR', 'DEC'):
                    val = val.strip()
                elif col['TYPE'] in ('INT4'):
                    val = int(val)
                else:
                    print(col)
                    print(val)
                    assert 0 == 1
                row.append(val)
                offset += l
            data.append(row)
            buf = None
        return ret['E_NO_MORE_DATA'], data
