{% extends "rotate_table.sql" %}
{% block query %}
SELECT author.author_name,
    COUNT(page_views.page_view_id) page_views,
    COUNT(quotes.quote) quotes
FROM scraped_quotes.authors
LEFT JOIN scraped_quotes.page_views
    USING(author_name)
LEFT JOIN scraped_quotes.quotes
    ON author.author = authors.author_name
{% endblock %}