#!/usr/bin/env python3
"""
Zero-Dependency Git Push Utility (Uses Pure-Python Dulwich Engine)
Allows pushing commits to GitHub without needing git.exe installed on Windows.
"""

import argparse
import sys
from pathlib import Path
import dulwich.porcelain as porcelain
from dulwich.repo import Repo

BASE_DIR = Path(__file__).resolve().parent.parent


def main():
    parser = argparse.ArgumentParser(description="Push project to GitHub using pure Python")
    parser.add_argument("--url", type=str, required=True, help="GitHub repository URL (e.g. https://github.com/user/repo.git)")
    parser.add_argument("--token", type=str, default=None, help="GitHub Personal Access Token (optional if in URL)")
    parser.add_argument("--branch", type=str, default="main", help="Target branch name")
    args = parser.parse_args()

    repo = Repo(str(BASE_DIR))
    remote_url = args.url

    # If token is provided, embed it into the HTTPS URL
    if args.token and "https://" in remote_url and "@" not in remote_url:
        remote_url = remote_url.replace("https://", f"https://{args.token}@")

    print(f"Pushing to GitHub: {args.url} (Branch: {args.branch})...")
    try:
        # Check current branch
        head_ref = repo.head()
        print(f"Current HEAD commit: {head_ref.decode()}")

        # Push
        porcelain.push(repo, remote_url, refspecs=[f"HEAD:refs/heads/{args.branch}"])
        print("\nSUCCESS! Successfully pushed to GitHub!")
        print(f"View your repository at: {args.url.replace('.git', '')}")
    except Exception as e:
        print(f"\nPush failed: {e}")
        print("\nTip: For private/protected repos, provide a GitHub Personal Access Token:")
        print("  python scripts/push_to_github.py --url <repo_url> --token <github_pat>")


if __name__ == "__main__":
    main()
