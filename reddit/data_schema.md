# BigQuery Schema for handy-implement-454013-q2.reddit.comments

| Field name | Type | Mode | Key | Collation | Default Value | Policy Tags | Data Policies | Description |
|------------|------|------|-----|-----------|---------------|-------------|---------------|-------------|
| id | STRING | NULLABLE | - | - | - | - | - | - |
| author | STRING | NULLABLE | - | - | - | - | - | - |
| created_utc | TIMESTAMP | NULLABLE | - | - | - | - | - | - |
| body | STRING | NULLABLE | - | - | - | - | - | - |
| score | INTEGER | NULLABLE | - | - | - | - | - | - |
| parent_id | STRING | NULLABLE | - | - | - | - | - | - |
| link_id | STRING | NULLABLE | - | - | - | - | - | - |
| subreddit_name_prefixed | STRING | NULLABLE | - | - | - | - | - | - |

# BigQuery Schema for handy-implement-454013-q2.reddit.submissions

| Field name | Type | Mode | Key | Collation | Default Value | Policy Tags | Data Policies | Description |
|------------|------|------|-----|-----------|---------------|-------------|---------------|-------------|
| id | STRING | NULLABLE | - | - | - | - | - | - |
| author | STRING | NULLABLE | - | - | - | - | - | - |
| created_utc | TIMESTAMP | NULLABLE | - | - | - | - | - | - |
| title | STRING | NULLABLE | - | - | - | - | - | - |
| selftext | STRING | NULLABLE | - | - | - | - | - | - |
| subreddit_name_prefixed | STRING | NULLABLE | - | - | - | - | - | - |
| num_comments | INTEGER | NULLABLE | - | - | - | - | - | - |
| score | INTEGER | NULLABLE | - | - | - | - | - | - |