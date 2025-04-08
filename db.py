from typing import Annotated
from fastapi import Depends
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session

import secret

username = secret.db_username
password = secret.db_password
engine = create_engine(f'mysql+mysqlconnector://{username}:{password}@localhost/edistynyttiedonhallinta_mqtt')

# muuttuja jolla luodaan yhteys
dw_session = sessionmaker(bind=engine)

def get_dw():
    _dw = None
    try:
        _dw = dw_session()
        yield _dw
    finally:
        if _dw is not None:
            _dw.close()

DW = Annotated[Session, Depends(get_dw)]