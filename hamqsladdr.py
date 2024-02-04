import asyncio
import argparse
import sys
import xml.etree.ElementTree as ET

from urllib.parse import urlencode

import aiohttp


class HamQTH:
    xmlurl = 'https://www.hamqth.com/xml.php?'
    xmlns = {'ns': 'https://www.hamqth.com'}
    
    def __init__(self, user, passwd):
        self._user = user
        self._passwd = passwd
        self._sessionid = None

    async def _make_request(self, client_session, **kwargs):
        params = urlencode(kwargs)
        async with client_session.get(self.xmlurl + params) as resp:
            resp.raise_for_status()
            root = ET.fromstring(await resp.read())
            err = root.find('ns:session/ns:error', self.xmlns)
            if err is not None:
                raise RuntimeError(err.text)
            return root

    async def login(self, client_session):
        result = await self._make_request(client_session, u=self._user, p=self._passwd)
        sessidelmt = result.find('ns:session/ns:session_id', self.xmlns)
        if sessidelmt is not None:
            self._sessionid = sessidelmt.text.strip()
        else:
            raise RuntimeError('No session id but no error specified!')

    async def lookup(self, client_session, callsign):
        result = await self._make_request(client_session, id=self._sessionid, callsign=callsign, prg='python')
        search = result.find('ns:search', self.xmlns)
        addr = {}
        for elmt in list(search):
            if elmt.tag == '{https://www.hamqth.com}adr_name':
                addr['name'] = elmt.text
            elif elmt.tag == '{https://www.hamqth.com}adr_street1':
                addr['street'] = elmt.text
            elif elmt.tag.startswith('{https://www.hamqth.com}adr_street'):
                raise RuntimeError('Cannot deal with multiple street addresses')
            elif elmt.tag == '{https://www.hamqth.com}adr_city':
                addr['city'] = elmt.text
            elif elmt.tag == '{https://www.hamqth.com}adr_zip':
                addr['zip'] = elmt.text
            elif elmt.tag == '{https://www.hamqth.com}adr_country':
                addr['country'] = elmt.text
            elif elmt.tag == '{https://www.hamqth.com}us_state':
                addr['state'] = elmt.text
        return addr


class QRZ:
    xmlurl = 'https://xmldata.qrz.com/xml/1.25/?'
    xmlns = {'ns': 'http://xmldata.qrz.com'}
    
    def __init__(self, user, passwd):
        self._user = user
        self._passwd = passwd
        self._sessionid = None

    async def _make_request(self, client_session, **kwargs):
        params = urlencode(kwargs)
        async with client_session.get(self.xmlurl + params) as resp:
            resp.raise_for_status()
            root = ET.fromstring(await resp.read())
            msg = root.find('ns:Session/ns:Message', self.xmlns)
            error = root.find('ns:Session/ns:Error', self.xmlns)
            if msg is not None:
                raise RuntimeError(msg.text)
            if error is not None:
                raise RuntimeError(error.text)
            return root

    async def login(self, client_session):
        result = await self._make_request(client_session, username=self._user, password=self._passwd)
        sessidelmt = result.find('ns:Session/ns:Key', self.xmlns)
        if sessidelmt is not None:
            self._sessionid = sessidelmt.text.strip()
        else:
            raise RuntimeError('No session id!')

    async def lookup(self, client_session, callsign):
        result = await self._make_request(client_session, s=self._sessionid, callsign=callsign, prg='tuxsoft')
        addr = {}
        fname = result.find('ns:Callsign/ns:fname', self.xmlns).text
        lname = result.find('ns:Callsign/ns:name', self.xmlns).text
        addr['name'] = f'{fname} {lname}'
        addr['street'] = result.find('ns:Callsign/ns:addr1', self.xmlns).text
        addr['city'] = result.find('ns:Callsign/ns:addr2', self.xmlns).text
        addr['zip'] = result.find('ns:Callsign/ns:zip', self.xmlns).text
        addr['country'] = result.find('ns:Callsign/ns:country', self.xmlns).text
        addr['state'] = result.find('ns:Callsign/ns:state', self.xmlns).text
        return addr


async def amain(svc, output_file_name, callsigns):
    async with aiohttp.ClientSession() as client_session:
        await svc.login(client_session)

        if output_file_name:
            output_file = open(args.output_file, 'w')
        else:
            output_file = sys.stdout

        with output_file:
            for callsign in callsigns:
                callsign = callsign.upper()
                try:
                    result = await svc.lookup(client_session, callsign)

                    print('{}:\n'
                          '\t{name}\n'
                          '\t{street}\n'
                          '\t{city} {state} {zip}\n'
                          '\t{country}\n'.format(callsign, **result))

                    if args.output_file:
                        if result['country'] == 'United States':
                            result['country'] = ''
                            print(','.join((callsign, result['name'], result['street'], result['city'], result['state'], result['zip'], result['country'])), file=output_file)

                except RuntimeError as e:
                    print('Failed to lookup callsign: {} - {}'.format(callsign, str(e)))


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-P', '--plugin', default='QRZ', choices=['QRZ', 'HamQTH'])
    parser.add_argument('-u', '--user', required=True)
    parser.add_argument('-p', '--passwd', required=True)
    parser.add_argument('-o', '--output-file')
    parser.add_argument('callsigns', nargs='+')
    args = parser.parse_args()

    svc = globals()[args.plugin](args.user, args.passwd)
    asyncio.run(amain(svc, args.output_file, args.callsigns))
