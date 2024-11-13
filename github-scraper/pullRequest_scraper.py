import requests
import pandas as pd
from datetime import datetime
import os
from typing import List, Dict, Optional
import logging
import time
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

class GitHubPullRequestService:
    def __init__(self, token: str):
        """
        Initialize the GitHub pull request service.
        
        Args:
            token (str): GitHub personal access token for authentication (required)
        """
        if not token:
            raise ValueError("GitHub token is required for accessing repositories")
            
        self.base_url = "https://api.github.com"
        self.headers = {
            "Authorization": f"token {token}",
            "Accept": "application/vnd.github.v3+json"
        }
        self.session = self._setup_session()
        self.logger = self._setup_logger()

    def _setup_session(self) -> requests.Session:
        """
        Set up a requests session with retry strategy and rate limit handling.
        """
        session = requests.Session()
        retry_strategy = Retry(
            total=5,  # number of retries
            backoff_factor=1,  # wait 1, 2, 4, 8, 16 seconds between retries
            status_forcelist=[429, 500, 502, 503, 504],  # HTTP status codes to retry on
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        session.headers.update(self.headers)
        return session
        
    def _setup_logger(self) -> logging.Logger:
        """Set up logging configuration."""
        logger = logging.getLogger("GitHubPullRequestService")
        logger.setLevel(logging.INFO)
        
        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            logger.addHandler(handler)
            
        return logger
    
    def _handle_rate_limit(self, response: requests.Response):
        """
        Handle GitHub API rate limiting.
        
        Args:
            response (requests.Response): Response from GitHub API
        """
        remaining = int(response.headers.get('X-RateLimit-Remaining', 0))
        reset_time = int(response.headers.get('X-RateLimit-Reset', 0))
        
        if remaining <= 1:
            wait_time = reset_time - int(time.time())
            if wait_time > 0:
                self.logger.warning(f"Rate limit nearly exceeded. Waiting {wait_time} seconds...")
                time.sleep(wait_time)

    def _check_repo_access(self, owner: str, repo: str) -> bool:
        """
        Check if the repository is accessible with current credentials.
        """
        url = f"{self.base_url}/repos/{owner}/{repo}"
        response = self.session.get(url)
        
        if response.status_code == 200:
            repo_data = response.json()
            self.logger.info(f"Successfully accessed repository: {repo_data['full_name']}")
            self.logger.info(f"Repository visibility: {'private' if repo_data['private'] else 'public'}")
            return True
        else:
            self.logger.error(f"Failed to access repository: {response.status_code} - {response.text}")
            return False

    def get_pull_requests(self, owner: str, repo: str, state: Optional[str] = 'all') -> List[Dict]:
        """
        Fetch all pull requests (open, closed, or all) from a specific repository with rate limit handling.
        """
        if not self._check_repo_access(owner, repo):
            raise Exception("Repository not accessible. Please check your permissions and token.")
            
        pull_requests = []
        page = 1
        per_page = 100
        
        params = {
            "state": state,  # "open", "closed", or "all"
            "per_page": per_page,
            "page": page
        }

        while True:
            url = f"{self.base_url}/repos/{owner}/{repo}/pulls"
            self.logger.info(f"Fetching page {page} of pull requests...")
            try:
                response = self.session.get(url, params=params)
                self._handle_rate_limit(response)
                response.raise_for_status()
                
                page_pulls = response.json()
                self.logger.debug(f"Retrieved {len(page_pulls)} pull requests from page {page}")
                
                if not page_pulls:
                    break
                
                pull_requests.extend(page_pulls)
                page += 1
                params["page"] = page
                
                # Add a small delay between requests to be nice to GitHub's API
                time.sleep(0.5)
                
            except requests.exceptions.RequestException as e:
                self.logger.error(f"Error fetching pull requests: {str(e)}")
                raise Exception(f"GitHub API error: {str(e)}")
        
        if not pull_requests:
            self.logger.warning("No pull requests found in the repository")
            raise Exception("No pull requests found in the repository.")
            
        self.logger.info(f"Successfully fetched {len(pull_requests)} pull requests")
        return pull_requests
    
    def process_pull_requests(self, pull_requests: List[Dict]) -> pd.DataFrame:
        """
        Process pull request data into a pandas DataFrame.
        """
        if not pull_requests:
            raise Exception("No pull requests to process")
            
        processed_pull_requests = []
        total_pull_requests = len(pull_requests)
        
        for index, pr in enumerate(pull_requests, 1):
            try:
                if index % 10 == 0:
                    self.logger.info(f"Processing pull request {index}/{total_pull_requests}")
                    
                pr_data = {
                    'pr_number': pr['number'],
                    'title': pr['title'],
                    'user': pr['user']['login'],
                    'state': pr['state'],
                    'created_at': pr['created_at'],
                    'updated_at': pr['updated_at'],
                    'merged_at': pr['merged_at'],
                    'url': pr['html_url'],
                    'comments': pr.get('comments', 0),
                    'review_comments': pr.get('review_comments', 0),
                    'additions': pr.get('additions', 0),
                    'deletions': pr.get('deletions', 0),
                    'changed_files': pr.get('changed_files', 0)
                }
                
                processed_pull_requests.append(pr_data)
                
            except KeyError as e:
                self.logger.error(f"Error processing pull request {pr.get('number', 'unknown')}: {str(e)}")
                continue
            
        if not processed_pull_requests:
            raise Exception("No pull requests could be processed successfully")
            
        df = pd.DataFrame(processed_pull_requests)
        df['created_at'] = pd.to_datetime(df['created_at'])
        df['updated_at'] = pd.to_datetime(df['updated_at'])
        df['merged_at'] = pd.to_datetime(df['merged_at'])
        return df.sort_values('created_at', ascending=False)
    
    # def save_pull_requests_to_csv(self, owner: str, repo: str, output_path: str = None, state: str = 'all') -> str:
    #     """
    #     Get pull requests from a repository and save them to a CSV file.
    #     """
    #     pull_requests = self.get_pull_requests(owner, repo, state)
    #     df = self.process_pull_requests(pull_requests)
        
    #     if output_path is None:
    #         output_path = f"{owner}_{repo}_pull_requests_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
            
    #     df.to_csv(output_path, index=False)
    #     self.logger.info(f"Pull requests saved to: {output_path}")
    #     return output_path

    def save_pull_requests_to_json(self, owner: str, repo: str, output_path: str = None,
                        state: str = 'all') -> str:
        """
        Get issues from a repository and save them to a JSON file.
        """
        pull_requests = self.get_pull_requests(owner, repo, state)
        df = self.process_pull_requests(pull_requests)
        
        if output_path is None:
            output_path = f"{owner}_{repo}_pull-requests_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        
        # Save the DataFrame as a JSON file
        df.to_json(output_path, orient='records', lines=True)  # Save one record per line
        self.logger.info(f"Issues saved to: {output_path}")
        return output_path

# Example usage
if __name__ == "__main__":
    import os
    from dotenv import load_dotenv
    
    # Load token from environment variable
    load_dotenv()
    token = os.getenv("GITHUB_TOKEN")
    
    if not token:
        print("Please set GITHUB_TOKEN environment variable")
        exit(1)
    
    try:
        # Initialize the service with token
        service = GitHubPullRequestService(token)
        
        # Example repository (replace with your repository details)
        owner = "prometheus-community"
        repo = "helm-charts"
        
        # Optional state for pull requests
        state = 'all'  # "open", "closed", or "all"
        
        output_file = service.save_pull_requests_to_json(
            owner,
            repo,
            state=state
        )
        print(f"Pull requests saved to: {output_file}")
        
    except Exception as e:
        print(f"Error: {str(e)}")
