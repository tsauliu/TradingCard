# Reddit Data Analysis Project - CLAUDE.md

## Project Overview
This project involves generating BigQuery scripts to analyze Reddit data stored in Google Cloud, downloading the results, and performing data analysis. The workflow involves Claude generating SQL queries, the user executing them on Google Cloud Platform, and then providing the results back for analysis.

## Database Schema

### Tables
- **handy-implement-454013-q2.reddit.submissions** - Reddit submission posts
- **handy-implement-454013-q2.reddit.comments** - Reddit comments
    
### Key Fields
**Submissions:**
- `id`: Unique submission identifier
- `author`: Username of submitter
- `created_utc`: Timestamp of submission
- `title`: Post title
- `selftext`: Post body text
- `subreddit_name_prefixed`: Subreddit (e.g., r/example)
- `num_comments`: Number of comments
- `score`: Post score/upvotes

**Comments:**
- `id`: Unique comment identifier
- `author`: Username of commenter
- `created_utc`: Timestamp of comment
- `body`: Comment text
- `score`: Comment score/upvotes
- `parent_id`: ID of parent comment/post
- `link_id`: ID of associated submission
- `subreddit_name_prefixed`: Subreddit

## Data Overview
Based on sample data:
- **Time Range**: Data goes back to at least 2007-2010
- **Content**: Multi-language content (English, Arabic, etc.)
- **Scale**: Large dataset spanning multiple years
- **Deleted Content**: Many entries show `[deleted]` for author/content

## Workflow
1. **Query Generation**: Claude creates BigQuery SQL scripts
2. **Execution**: User runs queries on Google Cloud Platform
3. **Data Download**: User downloads results and shares with Claude
4. **Analysis**: Claude analyzes the downloaded data and provides insights

## Files
- `data_schema.md`: Detailed schema documentation for both tables
- `sample_data_queries.sql`: Sample queries to understand data structure
- `sample_data_queries.md`: Sample query results for reference

## Common Query Patterns
- Time-based aggregations (daily, weekly, monthly)
- Content analysis across subreddits
- User activity patterns
- Comment-to-submission ratios
- Score/engagement metrics

## Notes
- All timestamps are in UTC
- Many historical entries have deleted authors/content
- Data includes international content from various subreddits
- Large scale dataset requiring efficient query design