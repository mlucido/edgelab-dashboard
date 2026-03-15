import time, requests, eth_account
from eth_keys import keys
from eth_utils import keccak
import os

print("eth_account version:", eth_account.__version__)

private_key = input("Enter your Polygon private key (0x...): ").strip()
if not private_key.startswith("0x"):
    private_key = "0x" + private_key

pk_bytes = bytes.fromhex(private_key[2:])
pk = keys.PrivateKey(pk_bytes)
address = pk.public_key.to_checksum_address()
print(f"Wallet: {address}")

ts = str(int(time.time()))
nonce = "0"

# Manual EIP-191 personal_sign
msg = ts.encode()
prefix = f"\x19Ethereum Signed Message:\n{len(msg)}".encode()
msg_hash = keccak(prefix + msg)
sig = pk.sign_msg_hash(msg_hash)
signature = "0x" + sig.to_bytes().hex()

headers = {
    "Content-Type": "application/json",
    "POLY_ADDRESS": address,
    "POLY_SIGNATURE": signature,
    "POLY_TIMESTAMP": ts,
    "POLY_NONCE": nonce,
}

print("Calling Polymarket CLOB...")
r = requests.post("https://clob.polymarket.com/auth/api-key", headers=headers)
print(f"HTTP {r.status_code}: {r.text}")

if r.status_code == 200:
    d = r.json()
    print(f"\n✅ SUCCESS:")
    print(f"POLYMARKET_API_KEY={d.get('apiKey','')}")
    print(f"POLYMARKET_API_SECRET={d.get('secret','')}")
    print(f"POLYMARKET_API_PASSPHRASE={d.get('passphrase','')}")
