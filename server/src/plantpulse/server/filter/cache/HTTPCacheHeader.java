package plantpulse.server.filter.cache;


/**
 * Cache HTTP response headers.
 *
 * @author Samuel Santos
 * @author John Yeary
 * @version 2.3.1
 */
public enum HTTPCacheHeader {
    /**
     * The Cache-Control general-header field is used to specify directives that MUST be obeyed by all caching
     * mechanisms along the request/response chain.
     */
    CACHE_CONTROL("Cache-Control"),
    /**
     * The Expires entity-header field gives the date/time after which the response is considered stale.
     */
    EXPIRES("Expires"),
    /**
     * The Pragma general-header field is used to include implementation- specific directives that might apply to any
     * recipient along the request/response chain.
     */
    PRAGMA("Pragma"),
    /**
     * The ETag response-header field provides the current value of the entity tag for the requested variant.
     */
    ETAG("ETag"),
    /**
     * The Vary field value indicates the set of request-header fields that fully determines, while the response is
     * fresh, whether a cache is permitted to use the response to reply to a subsequent request without revalidation.
     */
    VARY("Vary");

    private final String name;

    private HTTPCacheHeader(String name) {
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