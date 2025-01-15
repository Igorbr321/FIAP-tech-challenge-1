
def export_dataframe_to_csv(df, folder_path, file_name):
    """
    Exporta um DataFrame para um arquivo CSV em uma pasta específica.
    
    Parâmetros:
    - df (pd.DataFrame): O DataFrame a ser exportado.
    - folder_path (str): O caminho da pasta onde o arquivo será salvo.
    - file_name (str): O nome do arquivo CSV (deve incluir a extensão '.csv').
    
    Retorna:
    - str: Caminho completo do arquivo salvo.
    """
    # Verifica se a pasta existe, caso contrário, cria a pasta

    import os

    if not os.path.exists(folder_path):
        os.makedirs(folder_path)
    
    # Gera o caminho completo do arquivo
    file_path = os.path.join(folder_path, file_name)
    
    # Exporta o DataFrame para CSV
    df.to_csv(file_path, index=False, sep=',', encoding='utf-8-sig')
    
    print(f"Arquivo salvo em: {file_path}")
    return file_path



#---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------


def process_export_data(table, start_year, end_year):
    """
    Processa uma tabela de exportação e retorna um DataFrame com os dados no formato desejado.

    Parâmetros:
        table (DataFrame): Tabela de exportação com colunas representando anos e valores.
        start_year (int): Ano inicial para filtrar os dados.
        end_year (int): Ano final para filtrar os dados.

    Retorna:
        DataFrame: Novo dataframe com colunas ['País', 'Ano', 'Volume Litro', 'Valor'].
    """

    import pandas as pd
    import numpy as np

    new_data = {'País': [], 'Ano': [], 'Volume Litro': [], 'Valor': []}

    for column in table.columns[2:]:  # Considerando que as duas primeiras colunas não são de dados anuais
        year = column.split('.')[0]  # Extrai o ano do nome da coluna
        if year.isdigit():
            year = int(year)
            if start_year <= year <= end_year:
                new_data['Ano'].extend([year] * len(table))
                new_data['País'].extend(table['País'])
                new_data['Volume Litro'].extend(table[column])

                # Encontra coluna de valor correspondente
                valor_col = column + '.1'
                if valor_col in table.columns:
                    new_data['Valor'].extend(table[valor_col])
                else:
                    # Se a coluna de valor não existe, adiciona NaN
                    new_data['Valor'].extend([np.nan] * len(table))

    # Converte para DataFrame
    df = pd.DataFrame(new_data)

    # Filtra as linhas onde 'Volume Litro' ou 'Valor' são zero ou nulos
    df = df[~((df['Volume Litro'] == 0) | (df['Valor'] == 0) | (df['Volume Litro'].isna()) | (df['Valor'].isna()))]

    # Retorna o dataframe criado a partir dos dados processados
    return df



#---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------


paises_org = {
    'Alemanha': 'Alemanha',
    'Angola': 'Angola',
    'Antigua e Barbuda': 'Antigua e Barbuda',
    'Antilhas Holandesas': 'Antilhas Holandesas',
    'Argentina': 'Argentina',
    'Aruba': 'Aruba',
    'Austrália': 'Austrália',
    'Bahamas': 'Bahamas',
    'Bangladesh': 'Bangladesh',
    'Barbados': 'Barbados',
    'Bélgica': 'Bélgica',
    'Benin': 'Benin',
    'Bermudas': 'Bermudas',
    'Bolívia': 'Bolívia',
    'Bulgária': 'Bulgária',
    'Bósnia-Herzegovina': 'Bósnia-Herzegovina',
    'Cabo Verde': 'Cabo Verde',
    'Camarões': 'Camarões',
    'Canadá': 'Canadá',
    'Catar': 'Catar',
    'Cayman, Ilhas': 'Ilhas Cayman',
    'Chile': 'Chile',
    'China': 'China',
    'Chipre': 'Chipre',
    'Cingapura': 'Cingapura',
    'Colômbia': 'Colômbia',
    'Coreia do Sul': 'Coreia do Sul',
    'Costa Rica': 'Costa Rica',
    'Cuba': 'Cuba',
    'Curaçao': 'Curaçao',
    'Dinamarca': 'Dinamarca',
    'Dominica': 'Dominica',
    'El Salvador': 'El Salvador',
    'Emirados Árabes Unidos': 'Emirados Árabes Unidos',
    'Equador': 'Equador',
    'Espanha': 'Espanha',
    'Estados Unidos': 'Estados Unidos',
    'Estônia': 'Estônia',
    'Falkland (Malvinas)': 'Malvinas',
    'Filipinas': 'Filipinas',
    'Finlândia': 'Finlândia',
    'França': 'França',
    'Gana': 'Gana',
    'Gibraltar': 'Gibraltar',
    'Granada': 'Granada',
    'Grécia': 'Grécia',
    'Guatemala': 'Guatemala',
    'Guiana': 'Guiana',
    'Guiné Equatorial': 'Guiné Equatorial',
    'Guiné-Bissau': 'Guiné-Bissau',
    'Haiti': 'Haiti',
    'Honduras': 'Honduras',
    'Hong Kong': 'Hong Kong',
    'Hungria': 'Hungria',
    'Ilha de Man': 'Ilha de Man',
    'Iraque': 'Iraque',
    'Irlanda': 'Irlanda',
    'Islândia': 'Islândia',
    'Itália': 'Itália',
    'Japão': 'Japão',
    'Jordânia': 'Jordânia',
    'Letônia': 'Letônia',
    'Libéria': 'Libéria',
    'Luxemburgo': 'Luxemburgo',
    'Líbano': 'Líbano',
    'Maldivas': 'Maldivas',
    'Malta': 'Malta',
    'Ilhas Marshall': 'Ilhas Marshall',
    'Montenegro': 'Montenegro',
    'México': 'México',
    'Nicarágua': 'Nicarágua',
    'Nigéria': 'Nigéria',
    'Noruega': 'Noruega',
    'Nova Zelândia': 'Nova Zelândia',
    'Outros': 'Outros',
    'Panamá': 'Panamá',
    'Paraguai': 'Paraguai',
    'Países Baixos': 'Países Baixos',
    'Peru': 'Peru',
    'Polônia': 'Polônia',
    'Porto Rico': 'Porto Rico',
    'Portugal': 'Portugal',
    'Quênia': 'Quênia',
    'Reino Unido': 'Reino Unido',
    'República Dominicana': 'República Dominicana',
    'República Tcheca': 'República Tcheca',
    'Rússia': 'Rússia',
    'Serra Leoa': 'Serra Leoa',
    'Singapura': 'Singapura',
    'Suriname': 'Suriname',
    'Suécia': 'Suécia',
    'Suíça': 'Suíça',
    'Tailândia': 'Tailândia',
    'Taiwan': 'Taiwan',
    'Trinidade e Tobago': 'Trinidade e Tobago',
    'Turquia': 'Turquia',
    'Uruguai': 'Uruguai',
    'Vanuatu': 'Vanuatu',
    'Venezuela': 'Venezuela',
    'Vietnã': 'Vietnã',
    'África do Sul': 'África do Sul',
    'Índia': 'Índia',
    'República Democrática da Alemanha': 'Alemanha',
    'Antígua e Barbuda': 'Antígua e Barbuda',
    'Argélia': 'Argélia',
    'Arábia Saudita': 'Arábia Saudita',
    'Barein': 'Barein',
    'Belize': 'Belize',
    'Birmânia': 'Birmânia',
    'Bélgica': 'Bélgica',
    'Canadá': 'Canadá',
    'Colômbia': 'Colômbia',
    'Congo': 'Congo',
    'Coreia do Sul': 'Coreia do Sul',
    'Costa do Marfim': 'Costa do Marfim',
    'Coveite': 'Coveite',
    'Dominica, Ilha de': 'Dominica',
    'Emirados Árabes Unidos': 'Emirados Árabes Unidos',
    'Finlândia': 'Finlândia',
    'Guiana Francesa': 'Guiana Francesa',
    'Guiné Equatorial': 'Guiné Equatorial',
    'Guiné Bissau': 'Guiné Bissau',
    'Índia': 'Índia',
    'Indonésia': 'Indonésia',
    'Irã': 'Irã',
    'Israel': 'Israel',
    'Iugoslávia': 'Iugoslávia',
    'Jamaica': 'Jamaica',
    'Líbia': 'Líbia',
    'Malásia': 'Malásia',
    'Mauritânia': 'Mauritânia',
    'Moçambique': 'Moçambique',
    'Mônaco': 'Mônaco',
    'Namíbia': 'Namíbia',
    'Nova Caledônia': 'Nova Caledônia',
    'Paquistão': 'Paquistão',
    'Países Baixos': 'Países Baixos',
    'República Centro-Africana': 'República Centro-Africana',
    'República Dominicana': 'República Dominicana',
    'Federação da Rússia': 'Rússia',
    'Senegal': 'Senegal',
    'Sri Lanka': 'Sri Lanka',
    'São Tomé e Príncipe': 'São Tomé e Príncipe',
    'Tanzânia': 'Tanzânia',
    'Togo': 'Togo',
    'Toquelau': 'Toquelau',
    'Áustria': 'Áustria',
    'África do Sul': 'África do Sul',
    'Alemanha, República Democrática': 'Alemanha',
    'Arábia Saudita': 'Arábia Saudita',
    'Bahrein': 'Bahrein',
    'Brasil': 'Brasil',
    'Bulgária': 'Bulgária',
    'Burquina Faso': 'Burquina Faso',
    'Bósnia': 'Bósnia',
    'Comores': 'Comores',
    'Cocos (Keeling), Ilhas': 'Ilhas Cocos (Keeling)',
    'Cook, Ilhas': 'Ilhas Cook',
    'Coreia do Norte': 'Coreia do Norte',
    'Croácia': 'Croácia',
    'Djibuti': 'Djibuti',
    'Egito': 'Egito',
    'Eslovênia': 'Eslovênia',
    'Falkland (Ilhas Malvinas)': 'Malvinas',
    'Faroé, Ilhas': 'Ilhas Faroé',
    'Gabão': 'Gabão',
    'Geórgia': 'Geórgia',
    'Guadalupe': 'Guadalupe',
    'Ilhas Virgens': 'Ilhas Virgens',
    'Jérsei': 'Jérsei',
    'Lituânia': 'Lituânia',
    'Macedônia': 'Macedônia',
    'Marrocos': 'Marrocos',
    'Martinica': 'Martinica',
    'Maurício': 'Maurício',
    'México': 'México',
    'Mongólia': 'Mongólia',
    'Omã': 'Omã',
    'Palau': 'Palau',
    'Pitcairn': 'Pitcairn',
    'Provisão de Navios e Aeronaves': 'Provisão de Navios e Aeronaves',
    'Quirguistão': 'Quirguistão',
    'Romênia': 'Romênia',
    'Rússia, Federação da': 'Rússia',
    'Samoa Americana': 'Samoa Americana',
    'São Cristóvão e Névis': 'São Cristóvão e Névis',
    'São Vicente e Granadinas': 'São Vicente e Granadinas',
    'Taiwan': 'Taiwan',
    'Trindade e Tobago': 'Trinidade e Tobago',
    'Turcas e Caicos, Ilhas': 'Ilhas Turcas e Caicos',
    'Tuvalu': 'Tuvalu',
    'Afeganistão': 'Afeganistão',
    'Anguilla': 'Anguilla',
    'Belize': 'Belize',
    'Comores': 'Comores',
    'Coreia, República Sul': 'Coreia do Sul',
    'Eslovaca, República': 'Eslováquia',
    'Estônia': 'Estônia',
    'Guiné Bissau': 'Guiné-Bissau',
    'Macau': 'Macau',
    'Malawi': 'Malawi',
    'Suazilândia': 'Suazilândia',
    'Trinidade Tobago': 'Trinidade e Tobago',
    'Tunísia': 'Tunísia'
}

#---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

def update_country_names(df, paises_org):
    """
    Atualiza os nomes dos países na coluna 'País' de acordo com o dicionário fornecido.

    Parâmetros:
        df (DataFrame): DataFrame com os dados contendo a coluna 'País'.
        paises_org (dict): Dicionário com os países originais como chaves e os países atualizados como valores.

    Retorna:
        DataFrame: Novo DataFrame com os nomes dos países atualizados.
    """
    df['País'] = df['País'].replace(paises_org)
    return df