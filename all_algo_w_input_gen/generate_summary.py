import pandas as pd

# ==========================
# Load dataset
# ==========================

df = pd.read_csv("final_results_v5.csv")

# ==========================
# Best by TIME
# ==========================

time_cols = {
    "Ring": "ring_time",
    "BiDirRing": "bidir_ring_time",
    "Bully": "bully_time",
    "Raft": "raft_time",
    "Echo": "echo_time"
}

time_df = df[list(time_cols.values())].copy()

# ignore failed elections (0 means no election)
time_df = time_df.replace(0, float("inf"))

best_time = time_df.idxmin(axis=1)

reverse_time = {v: k for k, v in time_cols.items()}
best_time = best_time.map(reverse_time)

print("\nTarget distribution (best by time):")
print(best_time.value_counts())

# ==========================
# Best by MESSAGE COUNT
# ==========================

msg_cols = {
    "Ring": "ring_msg_count",
    "BiDirRing": "bidir_ring_msg_count",
    "Bully": "bully_msg_count",
    "Raft": "raft_msg_count",
    "Echo": "echo_msg_count"
}

msg_df = df[list(msg_cols.values())].copy()

# ignore failed elections
msg_df = msg_df.replace(0, float("inf"))

best_msg = msg_df.idxmin(axis=1)

reverse_msg = {v: k for k, v in msg_cols.items()}
best_msg = best_msg.map(reverse_msg)

print("\nTarget distribution (best by messages):")
print(best_msg.value_counts())

# ==========================
# Valid samples
# ==========================

valid_time = (
    (df["ring_time"] > 0)
    | (df["bidir_ring_time"] > 0)
    | (df["bully_time"] > 0)
    | (df["raft_time"] > 0)
    | (df["echo_time"] > 0)
).sum()

valid_msg = (
    (df["ring_msg_count"] > 0)
    | (df["bidir_ring_msg_count"] > 0)
    | (df["bully_msg_count"] > 0)
    | (df["raft_msg_count"] > 0)
    | (df["echo_msg_count"] > 0)
).sum()

print(f"\nValid samples (time): {valid_time}")
print(f"Valid samples (messages): {valid_msg}")

# ==========================
# Features count
# ==========================

excluded = set(time_cols.values()) | set(msg_cols.values())

feature_count = len(
    [c for c in df.columns if c not in excluded]
)

print(f"Features used: {feature_count}")