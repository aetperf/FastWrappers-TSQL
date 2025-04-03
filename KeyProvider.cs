using System;
using System.IO;
using System.Security.Cryptography;

namespace FastWrapper 
{
	public static class KeyProvider
	{
		// Clé et IV en dur (exemple)
		private static readonly byte[] AesKey = {
			0x01, 0x33, 0x58, 0xA7, 0x3B, 0x99, 0x2D, 0xFA,
			0x62, 0x11, 0xD5, 0xE7, 0x8F, 0x2C, 0x99, 0x0A,
			0xF2, 0x68, 0x44, 0xFA, 0x48, 0x92, 0xBE, 0x65,
			0x10, 0x7A, 0xCA, 0xAC, 0x9E, 0xDE, 0x7F, 0x0C
		};

		private static readonly byte[] AesIV = {
			0x11, 0x22, 0xAA, 0x77, 0x55, 0x99, 0x10, 0x01,
			0x66, 0x33, 0x45, 0x0F, 0x3A, 0x2B, 0xCC, 0xEE
		};

		// Méthodes de chiffrement / déchiffrement

		public static string AesEncrypt(string plainText)
		{
			if (plainText == null) return null;
			using (Aes aes = Aes.Create())
			{
				aes.Key = AesKey;
				aes.IV = AesIV;
				aes.Mode = CipherMode.CBC;
				aes.Padding = PaddingMode.PKCS7;

				MemoryStream ms = new MemoryStream();
				using (ICryptoTransform encryptor = aes.CreateEncryptor())
				using (CryptoStream cs = new CryptoStream(ms, encryptor, CryptoStreamMode.Write))
				using (StreamWriter sw = new StreamWriter(cs))
				{
					sw.Write(plainText);
				}
				return Convert.ToBase64String(ms.ToArray());
			}
		}

		public static string AesDecrypt(string base64Cipher)
		{
			if (string.IsNullOrEmpty(base64Cipher)) return null;
			byte[] cipherBytes = Convert.FromBase64String(base64Cipher);
			using (Aes aes = Aes.Create())
			{
				aes.Key = AesKey;
				aes.IV = AesIV;
				aes.Mode = CipherMode.CBC;
				aes.Padding = PaddingMode.PKCS7;

				using (var ms = new MemoryStream(cipherBytes))
				using (var decryptor = aes.CreateDecryptor())
				using (var cs = new CryptoStream(ms, decryptor, CryptoStreamMode.Read))
				using (var sr = new StreamReader(cs))
				{
					return sr.ReadToEnd();
				}
			}
		}
	}
}
