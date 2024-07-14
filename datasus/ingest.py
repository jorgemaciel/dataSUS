# %%
import urllib.request
from tqdm import tqdm


def get_data(uf, ano, mes):

    url = f'ftp://ftp.datasus.gov.br/dissemin/publicos/SIHSUS/200801_/Dados/RD{uf}{ano}{mes}.dbc'
    files = f'../data/dbc/RD{uf}{ano}{mes}.dbc'

    urllib.request.urlretrieve(url, files)

# %%
def get_data_month(uf, datas):
    for i in tqdm(datas):
        ano, mes, dia = i.split('-')
        ano = ano[-2:]
        get_data(uf, ano, mes)

uf = 'CE'
datas = ['2024-01-01', '2024-02-01', '2024-03-01', '2024-04-01', '2024-05-01']
get_data_month(uf, datas)
# %%
