import re

import logging

logger = logging.getLogger(__name__)

def parse_event(event):
    ## Alert: New symbol: CROX was added to #B4-Scan#[BUY]
    ## Alert: New symbols: BILI, DDOG, IQ, LVS, NTES, ZM were added to #B4-Scan#[SELL]
    ## Alert: New symbols: APA, CENN, DOCU, NYCB, PATH, PYPL, STNE, UBER were added to #B4-Scan#[BUY]
    symbols, action, side = None, None, None
    actions = re.split("\\. ", event)
    rs =[]
    for a in actions:
        a = a.strip()
        matches = re.findall(r'.*[s|S]+ymbols?:(.+) [were|was]+ ([added|removed]+) [to|from]+ #B4#\[([BUY|SELL]+)]#(V.*)#.*',a)
        match = matches[0] if matches else None
        if match:
            symbols = match[0].split(',')
            symbols = [t.strip() for t in symbols]
            action = match[1]
            side = match[2]
            version = match[3]
            print(f'symbols = {symbols}, action = {action}, side = {side}, version = {version}')
            logger.info(f'parsed event : ({symbols}, {action}, {side}, {version})')
            rs.append((symbols, action, side, version))
        elif a:
            logger.info(f'No matched action found in event: {a}')

    return rs



def test_regx_matching():
    ## Alert: New symbol: CROX was added to #B4-Scan#[BUY]
    ## Alert: New symbols: BILI, DDOG, IQ, LVS, NTES, ZM were added to #B4-Scan#[SELL]
    ## Alert: New symbols: APA, CENN, DOCU, NYCB, PATH, PYPL, STNE, UBER were added to #B4-Scan#[BUY]
    ## Alert: New symbols: CLOV, SAN, SBLK were added to #B4-Scan#[SELL]. Symbols: GOEV, TMUS were removed from #B4-Scan#[SELL].
    event = "Alert: New symbols: BILI, DDOG, IQ, LVS, NTES, ZM were added to #B4-Scan#[SELL]."
    event = "Alert:  Symbol: MGP was removed from #B4-Scan#[BUY]."
    event = "Alert:  Symbols: AA, PAAS, UNM, WBA were removed from #B4-Scan#[SELL]."
    event = "Alert: New symbols: APA, CENN, DOCU, NYCB, PATH, PYPL, STNE, UBER were added to #B4-Scan#[BUY]."
    event = "Alert: New symbol: CROX was added to #B4-Scan#[BUY]."
    event = "Alert: New symbols: CLOV, SAN, SBLK were added to #B4-Scan#[SELL]. Symbols: GOEV, TMUS were removed from #B4-Scan#[SELL]."
    event = "Alert: New symbols: CLOV, SAN, SBLK were added to #B4-Scan#[SELL]. Symbols: GOEV, TMUS were removed from #B4-Scan#[SELL]."
    #event = "Alert: New symbols: CLOV, SAN, SBLK were added to #B4-Scan#[SELL]."
    event = "Alert: Symbols: CFG, CUK, MARA, SBLK, TIGR, UBS were removed from #B4#[BUY]#V5.3.1#."
    event = "Alert: New symbols: HYD, ME, TFI were added to #B4#[BUY]#V5.3.1#. Symbols: SEAT, VDE, ZI were removed from #B4#[BUY]#V5.3.1#."
    event = "Alert: Symbols: CFG, CUK, MARA, SBLK, TIGR, UBS were removed from #B4#[BUY]#V5.3.1#."

    actions = re.split("\\. ", event)

    for a in actions:
        matches = re.findall(r'.*[s|S]+ymbols?:(.+) [were|was]+ ([added|removed]+) [to|from]+ #B4#\[([BUY|SELL]+)]#(V.*)#.*', a)
        match = matches[0] if matches else None
        if match:
            symbols = match[0].split(',')
            symbols = [t.strip() for t in symbols]
            action = match[1]
            side = match[2]
            version = match[3]
            print(f'symbols = {symbols}, action = {action}, side = {side}, version = {version}')


##test_regx_matching()