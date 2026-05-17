import pandas as pd
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.backends import default_backend
import os
import json
import base64
from datetime import datetime
import numpy as np

def convert_numpy(obj):
    if isinstance(obj, np.integer):
        return int(obj)
    elif isinstance(obj, np.floating):
        return float(obj)
    elif isinstance(obj, np.bool_):
        return bool(obj)
    elif isinstance(obj, np.ndarray):
        return obj.tolist()
    raise TypeError(f"Type {type(obj)} not serializable")

print("=" * 60)
print("BUOC 4: DATA SECURITY & QUALITY MONITORING")
print("=" * 60)

# Doc data
print("\n--- DOC DATA ---")
df = pd.read_csv("./data/fraud_dataset.csv")
print(f"✅ Doc {len(df):,} records")

# ============================================================
# AES-256 ENCRYPTION
# ============================================================
print("\n--- AES-256 ENCRYPTION ---")

class AESEncryption:
    def __init__(self, key_size=256):
        self.key = os.urandom(key_size // 8)
        print(f"✅ AES-{key_size} initialized ({len(self.key)} bytes)")
    
    def encrypt(self, plaintext: str) -> dict:
        iv = os.urandom(16)
        cipher = Cipher(algorithms.AES(self.key), modes.CBC(iv), backend=default_backend())
        encryptor = cipher.encryptor()
        plaintext_bytes = plaintext.encode('utf-8')
        padding_length = 16 - (len(plaintext_bytes) % 16)
        padded = plaintext_bytes + bytes([padding_length] * padding_length)
        ciphertext = encryptor.update(padded) + encryptor.finalize()
        return {
            "ciphertext": base64.b64encode(ciphertext).decode(),
            "iv": base64.b64encode(iv).decode()
        }

aes = AESEncryption(key_size=256)

# ============================================================
# QUALITY MONITORING
# ============================================================
print("\n--- DATA QUALITY MONITORING ---")

quality_checks = {
    "R001_amount_not_null": {
        "passed": df["transaction_amount"].isnull().sum() == 0,
        "failed": df["transaction_amount"].isnull().sum(),
        "desc": "Amount khong duoc null"
    },
    "R002_txn_id_unique": {
        "passed": df["transaction_id"].nunique() == len(df),
        "failed": len(df) - df["transaction_id"].nunique(),
        "desc": "Transaction_id phai unique"
    },
    "R003_amount_range": {
        "passed": ((df["transaction_amount"] >= -10000000) & (df["transaction_amount"] <= 10000000)).all(),
        "failed": len(df[(df["transaction_amount"] < -10000000) | (df["transaction_amount"] > 10000000)]),
        "desc": "Amount trong [-10M, +10M]"
    },
}

passed = 0
results = []

print("Quality checks:")
for rule_id, rule in quality_checks.items():
    status = "✅ PASS" if rule["passed"] else "❌ FAIL"
    if rule["passed"]:
        passed += 1
    print(f"  {status} {rule['desc']}")
    results.append({"rule_id": rule_id, "passed": rule["passed"]})

print(f"\n📊 Summary: {passed}/{len(quality_checks)} checks passed")

# ============================================================
# USER RIGHTS MANAGEMENT
# ============================================================
print("\n--- USER RIGHTS MANAGEMENT ---")

user_roles = {
    "admin": ["read", "write", "decrypt", "export"],
    "analyst": ["read", "decrypt", "export"],
    "viewer": ["read"]
}

print("User roles:")
for role, perms in user_roles.items():
    print(f"  {role}: {', '.join(perms)}")

# ============================================================
# FRAUD ANALYSIS
# ============================================================
print("\n--- FRAUD ANALYSIS ---")

fraud_stats = {
    "total": len(df),
    "fraud": int(df["is_fraud"].sum()),
    "normal": int(len(df) - df["is_fraud"].sum()),
    "fraud_rate_pct": float(df["is_fraud"].mean() * 100)
}

print(f"Total: {fraud_stats['total']:,}")
print(f"Fraud: {fraud_stats['fraud']:,}")
print(f"Rate: {fraud_stats['fraud_rate_pct']:.2f}%")

# ============================================================
# LUU REPORT
# ============================================================
print("\n--- LUU REPORT ---")

report = {
    "timestamp": datetime.now().isoformat(),
    "quality_checks": results,
    "fraud_stats": fraud_stats,
    "user_roles": user_roles
}

with open("./results/buoc_4_security_report.json", "w") as f:
    json.dump(report, f, indent=2, ensure_ascii=False, default=convert_numpy)

print("✅ Report saved!")
print("\n✅ BUOC 4 HOAN THANH!")