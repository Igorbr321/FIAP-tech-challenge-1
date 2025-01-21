
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
    'Argentina': 'Argentina',
    'Aruba': 'Aruba',
    'Australia': 'Austrália',
    'Bahamas': 'Bahamas',
    'Bangladesh': 'Bangladesh',
    'Barbados': 'Barbados',
    'Belgica': 'Bélgica',
    'Benin': 'Benim',
    'Bermudas': 'Bermudas',
    'Bolívia': 'Bolívia',
    'Bósnia-Herzegovina': 'Bósnia-Herzegovina',
    'Cabo Verde': 'Cabo Verde',
    'Camarões': 'Camarões',
    'Canada': 'Canadá',
    'Catar': 'Catar',
    'Cayman, Ilhas': 'Ilhas Cayman',
    'Chile': 'Chile',
    'China': 'China',
    'Chipre': 'Chipre',
    'Cingapura': 'Singapura',
    'Colombia': 'Colômbia',
    'Coreia do Sul, Republica da': 'Coreia do Sul',
    'Cuba': 'Cuba',
    'Curaçao': 'Curaçau',
    'Dinamarca': 'Dinamarca',
    'Dominica': 'Dominica',
    'El Salvador': 'El Salvador',
    'Emirados Arabes Unidos': 'Emirados Árabes Unidos',
    'Equador': 'Equador',
    'Espanha': 'Espanha',
    'Estados Unidos': 'Estados Unidos',
    'Falkland (Malvinas)': 'Ilhas Malvinas',
    'Filipinas': 'Filipinas',
    'Filânldia': 'Finlândia',
    'França': 'França',
    'Gana': 'Gana',
    'Gibraltar': 'Gibraltar',
    'Granada': 'Granada',
    'Grécia': 'Grécia',
    'Guatemala': 'Guatemala',
    'Guiana': 'Guiana',
    'Guiné Equatorial': 'Guiné Equatorial',
    'Haiti': 'Haiti',
    'Hong Kong': 'Hong Kong',
    'Hungria': 'Hungria',
    'Ilha de Man': 'Ilha de Man',
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
    'Marshall, Ilhas': 'Ilhas Marshall',
    'Montenegro': 'Montenegro',
    'México': 'México',
    'Nigéria': 'Nigéria',
    'Noruega': 'Noruega',
    'Nova Zelândia': 'Nova Zelândia',
    'Panamá': 'Panamá',
    'Paraguai': 'Paraguai',
    'Países Baixos (Holanda)': 'Países Baixos',
    'Peru': 'Peru',
    'Polônia': 'Polônia',
    'Porto Rico': 'Porto Rico',
    'Portugal': 'Portugal',
    'Quênia': 'Quênia',
    'Reino Unido': 'Reino Unido',
    'Republica Tcheca': 'República Tcheca',
    'Rússia': 'Rússia',
    'Serra Leoa': 'Serra Leoa',
    'Singapura': 'Singapura',
    'Suriname': 'Suriname',
    'Suécia': 'Suécia',
    'Suíça': 'Suíça',
    'Tailândia': 'Tailândia',
    'Taiwan (Formosa)': 'Taiwan',
    'Tcheca, República': 'República Tcheca',
    'Trinidade e Tobago': 'Trinidad e Tobago',
    'Turquia': 'Turquia',
    'Uruguai': 'Uruguai',
    'Vanuatu': 'Vanuatu',
    'Venezuela': 'Venezuela',
    'Vietnã': 'Vietnã',
    'África do Sul': 'África do Sul',
    'Índia': 'Índia',
    'Alemanha, República Democrática da': 'Alemanha',
    'Alemanha, República Democrática': 'Alemanha',
    'Antígua e Barbuda': 'Antigua e Barbuda',
    'Argélia': 'Argélia',
    'Austrália': 'Austrália',
    'Barein': 'Barein',
    'Belize': 'Belize',
    'Bélgica': 'Bélgica',
    'Canadá': 'Canadá',
    'Colômbia': 'Colômbia',
    'Coreia do Sul': 'Coreia do Sul',
    'Costa Rica': 'Costa Rica',
    'Coveite': 'Catar',
    'Dominica, Ilha de': 'Dominica',
    'Emirados Árabes Unidos': 'Emirados Árabes Unidos',
    'Finlândia': 'Finlândia',
    'Guiana Francesa': 'Guiana Francesa',
    'Guine Equatorial': 'Guiné Equatorial',
    'India': 'Índia',
    'Indonésia': 'Indonésia',
    'Irã': 'Irã',
    'Líbia': 'Líbia',
    'Malásia': 'Malásia',
    'Moçambique': 'Moçambique',
    'Mônaco': 'Mônaco',
    'Nova Caledônia': 'Nova Caledônia',
    'Países Baixos': 'Países Baixos',
    'República Dominicana': 'República Dominicana',
    'Sri Lanka': 'Sri Lanka',
    'São Tomé e Príncipe': 'São Tomé e Príncipe',
    'Togo': 'Togo',
    'Toquelau': 'Toquelau',
    'Afeganistão': 'Afeganistão',
    'Antilhas Holandesas': 'Antilhas Holandesas',
    'Arábia Saudita': 'Arábia Saudita',
    'Belice': 'Belize',
    'Brasil': 'Brasil',
    'Bulgária': 'Bulgária',
    'Cocos (Keeling), Ilhas': 'Ilhas Cocos (Keeling)',
    'Comores': 'Comores',
    'Congo': 'Congo',
    'Coreia, Republica Sul': 'Coreia do Sul',
    'Croácia': 'Croácia',
    'Estônia': 'Estônia',
    'Guine Bissau': 'Guiné-Bissau',
    'Macau': 'Macau',
    'Malavi': 'Malawi',
    'Martinica': 'Martinica',
    'Mauritânia': 'Mauritânia',
    'Omã': 'Omã',
    'Palau': 'Palau',
    'Pitcairn': 'Pitcairn',
    'Suazilândia': 'Eswatini',
    'São Cristóvão e Névis': 'São Cristóvão e Névis',
    'São Vicente e Granadinas': 'São Vicente e Granadinas',
    'Trinidade Tobago': 'Trinidad e Tobago',
    'Tuvalu': 'Tuvalu',
    'Áustria': 'Áustria',
    'Africa do Sul': 'África do Sul',
    'Arabia Saudita': 'Arábia Saudita',
    'Burquina Faso': 'Burquina Faso',
    'Bósnia': 'Bósnia',
    'Camores': 'Comores',
    'Cook, Ilhas': 'Ilhas Cook',
    'Coreia do Norte': 'Coreia do Norte',
    'Costa do Marfim': 'Costa do Marfim',
    'Djibuti': 'Djibuti',
    'Egito': 'Egito',
    'Eslovênia': 'Eslovênia',
    'Falkland (Ilhas Malvinas)': 'Ilhas Malvinas',
    'Faroé, Ilhas': 'Ilhas Faroé',
    'Gabão': 'Gabão',
    'Georgia': 'Geórgia',
    'Honduras': 'Honduras',
    'Jérsei': 'Jersey',
    'Lituânia': 'Lituânia',
    'Macedônia': 'Macedônia',
    'Marrocos': 'Marrocos',
    'Mexico': 'México',
    'Mongólia': 'Mongólia',
    'Paquistão': 'Paquistão',
    'Provisão de Navios e Aeronaves': 'Provisão de Navios e Aeronaves',
    'Quirguistão': 'Quirguistão',
    'Romênia': 'Romênia',
    'Rússia,  Federação da': 'Rússia',
    'Samoa Americana': 'Samoa Americana',
    'Senegal': 'Senegal',
    'Taiwan': 'Taiwan',
    'Tanzânia': 'Tanzânia',
    'Turcas e Caicos, ilhas': 'Ilhas Turcas e Caicos',
    'Coreia do Norte, República' : 'Coreia do Norte',
    'Coreia do Sul, República' : 'Coreia do Sul',
    'Geórgia do Sul e Sandwich do Sul, Ilhas' : 'Georgia'
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



#--------------------------------------------------------------------------------------------

country_translation = {
    "Afghanistan": "Afeganistão",
    "Africa Eastern and Southern": "África Oriental e Austral",
    "Africa Western and Central": "África Ocidental e Central",
    "Albania": "Albânia",
    "Algeria": "Argélia",
    "American Samoa": "Samoa Americana",
    "Andorra": "Andorra",
    "Angola": "Angola",
    "Antigua and Barbuda": "Antígua e Barbuda",
    "Arab World": "Mundo Árabe",
    "Argentina": "Argentina",
    "Armenia": "Armênia",
    "Aruba": "Aruba",
    "Australia": "Austrália",
    "Austria": "Áustria",
    "Azerbaijan": "Azerbaijão",
    "Bahamas, The": "Bahamas",
    "Bahrain": "Bahrein",
    "Bangladesh": "Bangladesh",
    "Barbados": "Barbados",
    "Belarus": "Bielorrússia",
    "Belgium": "Bélgica",
    "Belize": "Belize",
    "Benin": "Benim",
    "Bermuda": "Bermudas",
    "Bhutan": "Butão",
    "Bolivia": "Bolívia",
    "Bosnia and Herzegovina": "Bósnia e Herzegovina",
    "Botswana": "Botsuana",
    "Brazil": "Brasil",
    "British Virgin Islands": "Ilhas Virgens Britânicas",
    "Brunei Darussalam": "Brunei",
    "Bulgaria": "Bulgária",
    "Burkina Faso": "Burquina Faso",
    "Burundi": "Burundi",
    "Cabo Verde": "Cabo Verde",
    "Cambodia": "Camboja",
    "Cameroon": "Camarões",
    "Canada": "Canadá",
    "Caribbean small states": "Pequenos estados caribenhos",
    "Cayman Islands": "Ilhas Cayman",
    "Central African Republic": "República Centro-Africana",
    "Central Europe and the Baltics": "Europa Central e os Bálticos",
    "Chad": "Chade",
    "Channel Islands": "Ilhas do Canal",
    "Chile": "Chile",
    "China": "China",
    "Colombia": "Colômbia",
    "Comoros": "Comores",
    "Congo, Dem. Rep.": "Congo, Rep. Dem.",
    "Congo, Rep.": "Congo",
    "Costa Rica": "Costa Rica",
    "Cote d'Ivoire": "Costa do Marfim",
    "Croatia": "Croácia",
    "Cuba": "Cuba",
    "Curacao": "Curaçao",
    "Cyprus": "Chipre",
    "Czechia": "República Tcheca",
    "Denmark": "Dinamarca",
    "Djibouti": "Djibuti",
    "Dominica": "Dominica",
    "Dominican Republic": "República Dominicana",
    "Early-demographic dividend": "Dividendo demográfico precoce",
    "East Asia & Pacific": "Ásia Oriental e Pacífico",
    "East Asia & Pacific (IDA & IBRD countries)": "Ásia Oriental e Pacífico (países IDA e IBRD)",
    "East Asia & Pacific (excluding high income)": "Ásia Oriental e Pacífico (excluindo alta renda)",
    "Ecuador": "Equador",
    "Egypt, Arab Rep.": "Egito, Rep. Árabe",
    "El Salvador": "El Salvador",
    "Equatorial Guinea": "Guiné Equatorial",
    "Eritrea": "Eritreia",
    "Estonia": "Estônia",
    "Eswatini": "Eswatini",
    "Ethiopia": "Etiópia",
    "Euro area": "Zona do Euro",
    "Europe & Central Asia": "Europa e Ásia Central",
    "Europe & Central Asia (IDA & IBRD countries)": "Europa e Ásia Central (países IDA e IBRD)",
    "Europe & Central Asia (excluding high income)": "Europa e Ásia Central (excluindo alta renda)",
    "European Union": "União Europeia",
    "Faroe Islands": "Ilhas Faroe",
    "Fiji": "Fiji",
    "Finland": "Finlândia",
    "Fragile and conflict affected situations": "Situações frágeis e afetadas por conflitos",
    "France": "França",
    "French Polynesia": "Polinésia Francesa",
    "Gabon": "Gabão",
    "Gambia, The": "Gâmbia",
    "Georgia": "Geórgia",
    "Germany": "Alemanha",
    "Ghana": "Gana",
    "Gibraltar": "Gibraltar",
    "Greece": "Grécia",
    "Greenland": "Groenlândia",
    "Grenada": "Granada",
    "Guam": "Guam",
    "Guatemala": "Guatemala",
    "Guinea": "Guiné",
    "Guinea-Bissau": "Guiné-Bissau",
    "Guyana": "Guiana",
    "Haiti": "Haiti",
    "Heavily indebted poor countries (HIPC)": "Países pobres altamente endividados (HIPC)",
    "High income": "Alta renda",
    "Honduras": "Honduras",
    "Hong Kong SAR, China": "Hong Kong",
    "Hungary": "Hungria",
    "IBRD only": "Somente IBRD",
    "IDA & IBRD total": "Total IDA e IBRD",
    "IDA blend": "Mistura IDA",
    "IDA only": "Somente IDA",
    "IDA total": "Total IDA",
    "Iceland": "Islândia",
    "India": "Índia",
    "Indonesia": "Indonésia",
    "Iran, Islamic Rep.": "Irã, Rep. Islâmica",
    "Iraq": "Iraque",
    "Ireland": "Irlanda",
    "Isle of Man": "Ilha de Man",
    "Israel": "Israel",
    "Italy": "Itália",
    "Jamaica": "Jamaica",
    "Japan": "Japão",
    "Jordan": "Jordânia",
    "Kazakhstan": "Cazaquistão",
    "Kenya": "Quênia",
    "Kiribati": "Quiribáti",
    "Korea, Dem. People's Rep.": "Rep. Dem. Popular da Coreia",
    "Korea, Rep.": "Coreia do Sul",
    "Kosovo": "Kosovo",
    "Kuwait": "Catar",
    "Kyrgyz Republic": "República Quirguistão",
    "Lao PDR": "Laos",
    "Late-demographic dividend": "Dividendo demográfico tardio",
    "Latin America & Caribbean": "América Latina e Caribe",
    "Latin America & Caribbean (excluding high income)": "América Latina e Caribe (excluindo alta renda)",
    "Latin America & the Caribbean (IDA & IBRD countries)": "América Latina e Caribe (países IDA e IBRD)",
    "Latvia": "Letônia",
    "Least developed countries: UN classification": "Países menos desenvolvidos: classificação da ONU",
    "Lebanon": "Líbano",
    "Lesotho": "Lesoto",
    "Liberia": "Libéria",
    "Libya": "Líbia",
    "Liechtenstein": "Liechtenstein",
    "Lithuania": "Lituânia",
    "Low & middle income": "Baixa e média renda",
    "Low income": "Baixa renda",
    "Lower middle income": "Renda média baixa",
    "Luxembourg": "Luxemburgo",
    "Macao SAR, China": "Macau, RAE da China",
    "Madagascar": "Madagáscar",
    "Malawi": "Malawi",
    "Malaysia": "Malásia",
    "Maldives": "Maldivas",
    "Mali": "Mali",
    "Malta": "Malta",
    "Marshall Islands": "Ilhas Marshall",
    "Mauritania": "Mauritânia",
    "Mauritius": "Maurício",
    "Mexico": "México",
    "Micronesia, Fed. Sts.": "Micronésia, Estados Federados",
    "Middle East & North Africa": "Oriente Médio e Norte da África",
    "Middle East & North Africa (IDA & IBRD countries)": "Oriente Médio e Norte da África (países IDA e IBRD)",
    "Middle East & North Africa (excluding high income)": "Oriente Médio e Norte da África (excluindo alta renda)",
    "Middle income": "Renda média",
    "Moldova": "Moldávia",
    "Monaco": "Mônaco",
    "Mongolia": "Mongólia",
    "Montenegro": "Montenegro",
    "Morocco": "Marrocos",
    "Mozambique": "Moçambique",
    "Myanmar": "Mianmar",
    "Namibia": "Namíbia",
    "Nauru": "Nauru",
    "Nepal": "Nepal",
    "Netherlands": "Países Baixos",
    "New Caledonia": "Nova Caledônia",
    "New Zealand": "Nova Zelândia",
    "Nicaragua": "Nicarágua",
    "Niger": "Níger",
    "Nigeria": "Nigéria",
    "North America": "América do Norte",
    "North Macedonia": "Macedônia do Norte",
    "Northern Mariana Islands": "Ilhas Marianas do Norte",
    "Norway": "Noruega",
    "Not classified": "Não classificado",
    "OECD members": "Membros da OCDE",
    "Oman": "Omã",
    "Other small states": "Outros pequenos estados",
    "Pacific island small states": "Pequenos estados insulares do Pacífico",
    "Pakistan": "Paquistão",
    "Palau": "Palau",
    "Panama": "Panamá",
    "Papua New Guinea": "Papua Nova Guiné",
    "Paraguay": "Paraguai",
    "Peru": "Peru",
    "Philippines": "Filipinas",
    "Poland": "Polônia",
    "Portugal": "Portugal",
    "Post-demographic dividend": "Dividendo demográfico pós",
    "Pre-demographic dividend": "Dividendo demográfico pré",
    "Puerto Rico": "Porto Rico",
    "Qatar": "Catar",
    "Romania": "Romênia",
    "Russian Federation": "Rússia",
    "Rwanda": "Ruanda",
    "Samoa": "Samoa",
    "San Marino": "San Marino",
    "Sao Tome and Principe": "São Tomé e Príncipe",
    "Saudi Arabia": "Arábia Saudita",
    "Senegal": "Senegal",
    "Serbia": "Sérvia",
    "Seychelles": "Seicheles",
    "Sierra Leone": "Serra Leoa",
    "Singapore": "Singapura",
    "Sint Maarten (Dutch part)": "Sint Maarten (parte holandesa)",
    "Slovak Republic": "República Eslovaca",
    "Slovenia": "Eslovênia",
    "Small states": "Pequenos estados",
    "Solomon Islands": "Ilhas Salomão",
    "Somalia": "Somália",
    "South Africa": "África do Sul",
    "South Asia": "Ásia do Sul",
    "South Asia (IDA & IBRD)": "Ásia do Sul (países IDA e IBRD)",
    "South Sudan": "Sudão do Sul",
    "Spain": "Espanha",
    "Sri Lanka": "Sri Lanka",
    "St. Kitts and Nevis": "São Cristóvão e Nevis",
    "St. Lucia": "Santa Lúcia",
    "St. Martin (French part)": "São Martinho (parte francesa)",
    "St. Vincent and the Grenadines": "São Vicente e Granadinas",
    "Sub-Saharan Africa": "África Subsaariana",
    "Sub-Saharan Africa (IDA & IBRD countries)": "África Subsaariana (países IDA e IBRD)",
    "Sub-Saharan Africa (excluding high income)": "África Subsaariana (excluindo alta renda)",
    "Sudan": "Sudão",
    "Suriname": "Suriname",
    "Sweden": "Suécia",
    "Switzerland": "Suíça",
    "Syrian Arab Republic": "República Árabe Síria",
    "Taiwan, China": "Taiwan",
    "Tajikistan": "Tajiquistão",
    "Tanzania": "Tanzânia",
    "Thailand": "Tailândia",
    "Timor-Leste": "Timor-Leste",
    "Togo": "Togo",
    "Tokelau": "Tokelau",
    "Tonga": "Tonga",
    "Trinidad and Tobago": "Trinidad e Tobago",
    "Tunisia": "Tunísia",
    "Turkiye": "Turquia",
    "Turkmenistan": "Turquemenistão",
    "Tuvalu": "Tuvalu",
    "Uganda": "Uganda",
    "Ukraine": "Ucrânia",
    "United Arab Emirates": "Emirados Árabes Unidos",
    "United Kingdom": "Reino Unido",
    "United States": "Estados Unidos",
    "Uruguay": "Uruguai",
    "Uzbekistan": "Uzbequistão",
    "Vanuatu": "Vanuatu",
    "Venezuela, RB": "Venezuela, RB",
    "Viet Nam": "Vietnã",
    "World": "Mundo",
    "Yemen, Rep.": "Iémen",
    "Zambia": "Zâmbia",
    "Zimbabwe": "Zimbábue"
}
