package au.uni.melb.cloud.computing.analytics.utils;

import java.util.function.BiFunction;

public enum Constants {
    DB_TWEETER("db_tweeter"),
    DB_GREEN_PLACES("db_visit_to_green_places"),
    DB_INCOME_WKLY("db_income_weekly"),
    DB_CRIME_DATA("db_crime_data"),
    DB_RENT_WKLY("db_rent_weekly"),

    Q_SENTIMENT("sentiment"),
    Q_TWEETS("tweets"),
    Q_SENTIMENT_WORK_LIFE("sentiment-work-life"),
    Q_SENTIMENT_VULGARITY("sentiment-vulgarity"),
    Q_SENTIMENT_GREEN_PLACES_VISIT("sentiment-green-places-visit"),
    Q_SENTIMENT_INCOME_WKLY("sentiment-income-weekly"),
    Q_SENTIMENT_CRIME("sentiment-crime"),
    Q_SENTIMENT_RENT_WKLY("sentiment-rent-weekly"),

    F_DATA("data"),
    F_SENTIMENT("sentiment"),
    F_VULGARITY("vulgarity"),
    F_REGION("region"),
    F_REGION_ID("regionId"),
    F_RECORD("record"),
    F_SENTIMENT_COUNT("sentiment-count"),
    F_SENTIMENT_VULGARITY_COUNT("sentiment-vulgar-count"),
    F_SENTIMENT_GREEN_PLACES_VISIT_COUNT("sentiment-green-places-visit-count"),
    F_SENTIMENT_INCOME_WKLY_COUNT("sentiment-income-wkly-count"),
    F_SENTIMENT_CRIME_COUNT("sentiment-crime-count"),
    F_SENTIMENT_RENT_WKLY_COUNT("sentiment-rent-wkly-count");

    private final String name;

    Constants(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }
}
