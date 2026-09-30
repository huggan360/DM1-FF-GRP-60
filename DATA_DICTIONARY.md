# Data dictionary and assumptions

The FriendFeed files have no header row. The preparation script assigns the
following names explicitly instead of relying on automatic type detection.

## Raw tables

### Entries (`entries1.csv`, `entries2.csv`, `entries3.csv`)

Tab-separated fields:

1. `post_id`
2. `author_id`
3. `source_name`
4. `source_url`
5. `geo_x`
6. `geo_y`
7. `posted_at`
8. `post_text`
9. `image_count`
10. `image_urls`
11. `video_count`
12. `video_urls`

### Comments (`commentAugSept.csv`)

Tab-separated fields:

1. `comment_id`
2. `entry_id` — the original post receiving the comment
3. `author_id`
4. `source_name`
5. `source_url`
6. `geo_x`
7. `geo_y`
8. `commented_at`
9. `comment_text`

The supplied README describes optional image/video fields for comments, but the
actual file consistently contains nine fields. The code follows the observed
file rather than the inconsistent documentation.

### Likes (`likes.csv`)

Tab-separated fields: `user_id`, `post_id`, `liked_at`.

### Following events (`followingAugSept.csv`)

Tab-separated fields: `followed_id`, `follower_id`, `observed_at`. The order
follows the Version 2 note in the supplied dataset documentation.

### Subscriptions (`subscriptions.csv`)

Comma-separated fields: `subscriber_id`, `subscribed_to_id`. This table is used
as the static follower graph. This direction is an explicit project assumption
and should be checked with the tutor if the original source documentation becomes
available.

### Services (`services.csv`)

Pipe-separated fields: `user_id`, `service_id`, `service_name`, `service_url`,
`username_on_service`, `user_url_on_service`.

### Users (`users.csv`)

Pipe-separated fields: `user_id`, `user_type`, `display_name`, `reserved_value`,
`user_description`.

## Missing and invalid values

Empty strings, `\N`, `null`, and `NULL` are read as missing. Identifiers are
trimmed. Timestamps and numeric values use safe casts so a malformed value becomes
missing rather than stopping the whole pipeline.

Quotation marks are treated as normal text rather than CSV control characters.
The source files use delimiters but do not consistently use CSV quoting. Rows with
the wrong number of fields are recorded under `data/quality/csv_rejects/` instead
of disappearing silently.

The Parquet files retain quality flags:

- `identifier_is_valid`
- `timestamp_in_study_window`, or `timestamp_is_plausible` for likes

Bad records are therefore auditable. Downstream analysis uses only records that
meet the relevant validity rules.

## Analysis table

`post_features.parquet` has one row per unique post ID. Important groups of
fields are:

- outcomes: `comment_count_7d`, `like_count_7d`, `engagement_count_7d`;
- labels: `high_engagement`, `high_reach_adjusted_engagement`;
- text structure: `character_count`, `word_count`, `hashtag_count`, `url_count`;
- media: `image_count`, `video_count`, `has_image`, `has_video`;
- time: `post_hour`, `weekday_name`, `is_weekend`, `time_of_day`;
- source: `source_name`, `source_group`;
- author: `author_post_count`, `follower_count`, `following_count`, and connected
  service/activity fields;
- quality: `has_full_7d_window`, `analysis_eligible`.

The raw text, profile names, user descriptions, and external URLs are deliberately
left out of the final feature table because the planned analysis does not need
them.

Reach-adjusted engagement is only calculated when the static network snapshot
contains a follower count for the author. Unknown follower counts are kept as
missing and are never treated as zero followers.
