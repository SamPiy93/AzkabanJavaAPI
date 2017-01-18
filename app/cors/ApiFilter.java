package cors;

import com.google.inject.Inject;
import play.filters.cors.CORSFilter;
import play.http.HttpFilters;
import play.mvc.EssentialAction;
import play.mvc.EssentialFilter;

/**
 * Filter to handle cors related http requests
 */
public class ApiFilter extends EssentialFilter implements HttpFilters {
    private CORSFilter corsFilter;

    @Inject
    public ApiFilter(CORSFilter corsFilter) {
        this.corsFilter = corsFilter;
    }

    @Override
    public EssentialFilter[] filters() {
        EssentialFilter[] essentialFilters = new EssentialFilter[1];
        essentialFilters[0] = this;
        return essentialFilters;
    }

    @Override
    public EssentialAction apply(EssentialAction next) {
        return corsFilter.asJava().apply(next);
    }
}
