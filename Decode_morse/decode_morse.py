'''
O Código Morse é um sistema de representação de letras, algarismos e sinais de pontuação através
de um sinal codificado enviado de modo intermitente. Foi desenvolvido por Samuel Morse em 1837, 
criador do telégrafo elétrico, dispositivo que utiliza correntes elétricas para controlar eletroímãs 
que atuam na emissão e na recepção de sinais. 
O script tem a finalidade de decifrar uma mensagem em código morse e salvá-la em texto claro.
'''

import os
import sys
import datetime
import pandas as pd
from config import file_path, dict_morse

def decode_morse(msg):
    '''
    input : mensagem em código morse com letras separadas por um espaço e palavras separadas por dois espaços
    output : palavra escrito em letras e algarismos
    '''
    # Divide a mensagem em palavras usando dois espaços como separador
    words = msg.split('  ')  # Dois espaços indicam separação de palavras
    decoded_message = []

    for word in words:
        if word:  # Verifica se a palavra não está vazia
            letters = word.split(' ')  # Um espaço indica separação de letras
            decoded_word = ''.join([dict_morse.get(letter, '?') for letter in letters])
            decoded_message.append(decoded_word)
        else:
            decoded_message.append(' ')  # Adiciona um espaço para manter a separação entre palavras
    
    return ' '.join(decoded_message)

def save_clear_msg_csv_hdr(msg_claro):
    '''
    input : mensagem em texto claro
    output : palavra escrito em letras e algarismos, salva em arquivo csv
    '''
    now = datetime.datetime.now()
    df = pd.DataFrame([[msg_claro, now]], columns=["mensagem", "datetime"])
    hdr = not os.path.exists(file_path)
    df.to_csv(file_path, mode="a", index=False, header=hdr)

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python script.py '<morse_code_message>'")
        sys.exit(1)
    
    msg_claro = decode_morse(sys.argv[1])
    save_clear_msg_csv_hdr(msg_claro)
