# Data description

The FriendFeed files do not have column headers. [`01_preprocessing.py`](scripts/01_preprocessing.py) gives each column a name, checks the values and saves the cleaned tables as Parquet files.

## Raw files

| File | Separator | Columns |
| --- | --- | --- |
| `entries1.csv` to `entries3.csv` | Tab | `post_id`, `author_id`, `source_name`, `source_url`, `geo_x`, `geo_y`, `posted_at`, `post_text`, `image_count`, `image_urls`, `video_count`, `video_urls` |
| `commentAugSept.csv` | Tab | `comment_id`, `entry_id`, `author_id`, `source_name`, `source_url`, `geo_x`, `geo_y`, `commented_at`, `comment_text` |
| `likes.csv` | Tab | `user_id`, `post_id`, `liked_at` |
| `followingAugSept.csv` | Tab | `followed_id`, `follower_id`, `observed_at` |
| `subscriptions.csv` | Comma | `subscriber_id`, `subscribed_to_id` |
| `services.csv` | Pipe | `user_id`, `service_id`, `service_name`, `service_url`, `username_on_service`, `user_url_on_service` |
| `users.csv` | Pipe | `user_id`, `user_type`, `display_name`, `reserved_value`, `user_description` |

`entry_id` in the comments table is the post receiving the comment. The comments file has nine columns, even though the original dataset notes mention optional image and video columns.

We use `subscriber_id` as the follower and `subscribed_to_id` as the account being followed. This makes `subscriptions.csv` the follower snapshot for the analysis.

## Cleaning

Empty values, `\N`, `null` and `NULL` become missing values. Invalid numbers and timestamps also become missing instead of stopping the pipeline. Rows with the wrong number of columns are saved in `data/quality/csv_rejects/` for checking.

The cleaned tables include `identifier_is_valid` and a timestamp quality flag. Invalid rows remain available for review but are not used in the analysis.

## Final post table

`data/parquet/features/post_features.parquet` contains one row for each unique post.

| Group | Main columns | Meaning |
| --- | --- | --- |
| Engagement | `like_count_7d`, `comment_count_7d`, `engagement_count_7d` | Reactions received during the first seven days |
| Labels | `high_engagement`, `high_reach_adjusted_engagement` | Whether a post passes the selected engagement threshold |
| Text | `character_count`, `word_count`, `hashtag_count`, `url_count` | Simple properties of the post text |
| Media | `image_count`, `video_count`, `has_image`, `has_video` | Image and video information |
| Time | `post_hour`, `weekday_name`, `is_weekend`, `time_of_day` | When the post was published |
| Source | `source_name`, `source_group` | Service used to publish the post |
| Author | `author_post_count`, `follower_count`, `following_count`, `connected_service_count` | Author activity and network size |
| Quality | `has_full_7d_window`, `analysis_eligible` | Whether the post can be compared fairly |

High engagement is based on the 99th percentile by default. Reach-adjusted engagement divides reactions by `follower_count + 1`. A missing follower count stays missing and is not treated as zero.

The final table does not include post text, profile names, descriptions or external URLs because the association-rule analysis does not use them.
