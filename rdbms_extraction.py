import extractionDetails
from AESCipher import AESCipher

if __name__ == '__main__':
    key = '[EX\xc8\xd5\xbfI{\xa2$\x05(\xd5\x18\xbf\xc0\x85)\x10nc\x94\x02)j\xdf\xcb\xc4\x94\x9d(\x9e'
    enc = AESCipher(key)
#   enc.encrypt_file('D:\\tmpFolderForCode\\password_file\\pwd.txt')
#   print(enc.decrypt_file('D:\\tmpFolderForCode\\password_file\\pwd.enc'))
    extractionDetails.extract_source_data(1, enc)
