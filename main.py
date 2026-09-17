import json
import time
import yaml
import asyncio
import aiohttp
import miaospeedlib

slave_cfg = {
    "address": "",
    "token": "",
    "path": "/",
    "invoker": "1145141919810",
    "tls": True,
    "skipCertVerify": True,
    "option": {
        "downloadDuration": 8,
        "downloadThreading": 8,
        "pingAverageOver": 5,
        "taskRetry": 3,
        "downloadURL": "DYNAMIC:INTL",
        "pingAddress": "https://gstatic.com/generate_204",
        "stunURL": "udp://stun.msl.la:3478",
        "taskTimeout": 2500,
        "dnsServer": [],
        "apiVersion": 1
    },
    "id": "MiaoKoBackend",
    "comment": "MiaoKoBackend",
    "hidden": False,
    "type": "miaospeed",
    "buildtoken": "MIAOKO4|580JxAo049R|GEnERAl|1X571R930|T0kEN",
}

async def startmod(self, reqdata: dict = None):
    start_time = time.strftime("%Y-%m-%dT%H-%M-%S", time.localtime())
    resdata = {}
    connected = False
    ws_scheme, verify_ssl = self.get_ws_opt()
    async with aiohttp.ClientSession() as session:
        try:
            async with session.ws_connect(f"{ws_scheme}://{self.host}:{self.port}{self.path}", verify_ssl=verify_ssl) as ws:
                self.sign_request()
                connected = True
                await ws.send_str(self.SlaveRequest.to_json())
                while True:
                    msg = await ws.receive()
                    if msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                        break
                    elif msg.type == aiohttp.WSMsgType.TEXT:
                        ms_data: dict = json.loads(msg.data)
                        if not ms_data:
                            continue
                        if 'Result' in ms_data and ms_data.get('Result', {}):
                            resdata = ms_data['Result']['Results']
                            break
                        elif 'Progress' in ms_data and ms_data.get('Progress', {}):
                            prograss = ms_data.get('Progress', {})
                            queuing = prograss.get('Queuing', 0)
                            if queuing:
                                print(f'Task Queue {queuing}')
                    elif msg.type == aiohttp.WSMsgType.BINARY:
                        pass
                await ws.close()
        except aiohttp.ClientConnectorError as e:
            print(str(e))
        except asyncio.TimeoutError:
            if connected:
                print('Connection Close')
            else:
                print('Connection Timeout')
        except Exception as e:
            print(str(e))
        except KeyboardInterrupt:
            pass
        finally:
            return resdata, start_time

miaospeedlib.MiaoSpeed.start = startmod

async def main():
    async def miaospeed_test(nodes):
        msreq = miaospeedlib.SlaveRequest(
            miaospeedlib.SlaveRequestBasics(
                ID=slave_cfg['id'],
                Slave="slave_cfg['id']",
                SlaveName="slave_cfg['comment']",
                Invoker=slave_cfg['invoker'],
                Version="4.3.3"
            ),
            miaospeedlib.SlaveRequestOptions(Matrices=[
                miaospeedlib.SlaveRequestMatrixEntry(miaospeedlib.SlaveRequestMatrixType.TEST_PING_RTT, ""),
                miaospeedlib.SlaveRequestMatrixEntry(miaospeedlib.SlaveRequestMatrixType.TEST_PING_CONN, ""),
            ]),
            miaospeedlib.SlaveRequestConfigs().from_option(miaospeedlib.MiaoSpeedOption().from_obj(slave_cfg['option']))
        )
        ms = miaospeedlib.MiaoSpeed(slave_config = miaospeedlib.MiaoSpeedSlave().from_obj(slave_cfg), slave_request = msreq, proxyconfig=nodes)
        
        ms.SlaveRequest.Configs.patch_version()
        res, start_time = await ms.start()
        return res, start_time
    
    with open(f"Clash.yaml", 'r', encoding="UTF-8") as fp:
        nodes = yaml.safe_load(fp)['proxies']
    res = []
    i = 0
    while True:
        if i + 2000 > len(nodes):
            restemp, start_time = await miaospeed_test(nodes[i:len(nodes)])
            res.extend(restemp)
            break
        restemp, start_time = await miaospeed_test(nodes[i:i + 2000])
        res.extend(restemp)
        i = i + 2001
    
    all_proxy = {}
    with open(f"Clash.yaml", 'r', encoding="UTF-8") as fp:
        data = yaml.safe_load(fp)
    for proxy in data["proxies"]:
        all_proxy[proxy["name"]] = proxy
    
    final = {'proxies': []}
    for proxy in res:
        if json.loads(proxy['Matrices'][0]['Payload'])['Value'] != 0 or json.loads(proxy['Matrices'][1]['Payload'])['Value'] != 0:
            try:
                final['proxies'].append(all_proxy[proxy['ProxyInfo']['Name']])
            except:
                pass
    with open(f"Clash.yaml", "w", encoding="UTF-8") as fp:
        yaml.safe_dump(final, fp, sort_keys = False, allow_unicode = True)
    

if __name__ == '__main__':
    asyncio.run(main())
