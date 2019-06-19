import base64
import hashlib
from Crypto import Random
from Crypto.Cipher import AES
import os


class AESCipher:
    def __init__(self, key):
        self.key = hashlib.sha256(key.encode()).digest()

    def _pad(self, s):
        return s + (AES.block_size - len(s) % AES.block_size) * chr(AES.block_size - len(s) % AES.block_size).encode("utf8")

    @staticmethod
    def _unpad(s):
        return s[:-ord(s[len(s) - 1:])]

    def encrypt(self, raw):
        raw = self._pad(raw)
        iv = Random.new().read(AES.block_size)
        cipher = AES.new(self.key, AES.MODE_CBC, iv)
        return base64.b64encode(iv + cipher.encrypt(raw))

    def encrypt_file(self, in_filename, out_filename=None):
        if not out_filename:
            out_filename = in_filename[:-4] + '.enc'

        with open(in_filename, 'rb') as f:
            plaintext = f.read()
        enc = self.encrypt(plaintext)

        with open(out_filename, 'wb') as f:
            f.write(enc)
    #   Uncomment below code to delete original password file after encryption
    #   os.remove(in_filename)

    def decrypt(self, enc):
        enc = base64.b64decode(enc)
        iv = enc[:AES.block_size]
        cipher = AES.new(self.key, AES.MODE_CBC, iv)
        return self._unpad(cipher.decrypt(enc[AES.block_size:])).decode('utf-8')

    def decrypt_file(self,enc_filename):
        with open(enc_filename, 'rb') as f:
            ciphertext = f.read()
        dec = self.decrypt(ciphertext)
        return dec
