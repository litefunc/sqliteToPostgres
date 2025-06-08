#!/bin/sh

FILE_DIR=$(cd $(dirname $0); pwd)

python3 $FILE_DIR/index.py
python3 $FILE_DIR/三大法人買賣超日報.py
python3 $FILE_DIR/大盤成交統計.py
python3 $FILE_DIR/景氣指標及燈號-指標構成項目.py
python3 $FILE_DIR/'當日融券賣出與借券賣出成交量值(元).py'
python3 $FILE_DIR/ifrs前後-綜合損益表.py
python3 $FILE_DIR/個股日本益比、殖利率及股價淨值比.py
python3 $FILE_DIR/大盤統計資訊.py
python3 $FILE_DIR/景氣指標及燈號-綜合指數.py
python3 $FILE_DIR/'自營商買賣超彙總表 (股).py'
python3 $FILE_DIR/ifrs前後-資產負債表-一般業.py
python3 $FILE_DIR/'外資及陸資買賣超彙總表 (股).py'
python3 $FILE_DIR/'投信買賣超彙總表 (股).py'
python3 $FILE_DIR/'每日收盤行情(全部(不含權證、牛熊證)).py'
python3 $FILE_DIR/除權息計算結果表.py