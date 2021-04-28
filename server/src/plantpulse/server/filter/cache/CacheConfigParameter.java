package plantpulse.server.filter.cache;

public enum CacheConfigParameter {
    /**
     * Cache directive to set an expiration time, in seconds, relative to the current date.
     */
    EXPIRATION("expiration"),
    /**
     * Cache directive to control where the response may be cached.
     */
    PRIVATE("private"),
    /**
     * Cache directive to define whether conditional requests are required or not for stale responses.
     */
    MUST_REVALIDATE("must-revalidate"),
    /**
     * Cache directive to instructs proxies to cache different versions of the same resource based on specific
     * request-header fields.
     */
    VARY("vary");

    private final String name;

    private CacheConfigParameter(String name) {
        this.name = name;
    }

    /**
     * Gets the parameter name.
     *
     * @return the parameter name
     */
    public String getName() {
        return this.name;
    }
}