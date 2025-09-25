
package ch.poole.osm.qa.address;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class GWRcompare {

    private static final int EARTH_RADIUS_EQUATOR = 6378137;
    private static final int EARTH_RADIUS_POLAR   = 6356752;
    /**
     * The arithmetic mean of the two WGS84 reference-ellipsoids.
     */
    private static final int EARTH_RADIUS         = (EARTH_RADIUS_EQUATOR + EARTH_RADIUS_POLAR) / 2;

    // percentage of addresses that have to have the official flag set for it to be considered valid
    private static final float DEFAULT_OFFICIAL_VALID_LIMIT = 0.8F;

    private static final int MATCHING_DISTANCE = 50;

    private static final String WARNINGS_DIR = "warnings";
    private static final String MISSING_DIR  = "missing";

    private static final String OUTPUT_OPT               = "output";
    private static final String USER_OPT                 = "user";
    private static final String PASSWORD_OPT             = "password";
    private static final String CONNECTION_OPT           = "connection";
    private static final String MUNICIPALITY_OPT         = "municipality";
    private static final String OFFICIAL_VALID_LIMIT_OPT = "limit";

    private static final String PASSWORD_PROP = "password";
    private static final String USER_PROP     = "user";

    private static final String LANG_IT               = "it";
    private static final String LANG_FR               = "fr";
    private static final String LANG_RM               = "rm";
    private static final String LANG_DE               = "de";
    private static final String GWR_LANG_IT           = "9904";
    private static final String GWR_LANG_FR           = "9903";
    private static final String GWR_LANG_RM           = "9902";
    private static final String GWR_LANG_DE           = "9901";
    private static final String SWISSTOPO_STREET_GEOM = "Street";

    private static final Pattern ANCILLARY_NUMBER = Pattern.compile("^[^\\.]+[\\.\\,].*$");

    interface GeoJsonOut {
        String toGeoJson();
    }

    private static class Address implements GeoJsonOut {

        String  osmGeom;
        long    osmId;
        String  housenumber;
        String  housename;
        String  street;
        String  streetDe;
        String  streetFr;
        String  streetIt;
        String  streetRm;
        String  place;
        String  streetType;
        String  streetLang;
        String  postcode;
        String  city;
        String  full;
        int     gwrCategory;
        int     gwrClass;
        boolean official;
        float   lon;
        float   lat;

        @Override
        public String toString() {
            return housenumber + " " + street;
        }

        public boolean isAncillary() {
            Matcher m = housenumber != null ? ANCILLARY_NUMBER.matcher(housenumber) : null;
            return gwrCategory == 1010 || gwrCategory == 1080 || gwrClass == 1242 || gwrClass == 1252 || (m != null && m.find());
        }

        /**
         * Just output the GWR fields
         * 
         * @return
         */
        @Override
        public String toGeoJson() {
            StringBuilder s = new StringBuilder();
            s.append("{\"type\":\"Feature\",\n");
            s.append("\"properties\":{");
            s.append("\"addr:housenumber\":\"" + housenumber + "\",");
            if (SWISSTOPO_STREET_GEOM.equals(streetType)) {
                s.append("\"addr:street\":\"" + (street != null ? street : "") + "\",");
                if (streetDe != null) {
                    s.append("\"addr:street:de\":\"" + streetDe + "\",");
                }
                if (streetFr != null) {
                    s.append("\"addr:street:fr\":\"" + streetFr + "\",");
                }
                if (streetIt != null) {
                    s.append("\"addr:street:it\":\"" + streetIt + "\",");
                }
                if (streetRm != null) {
                    s.append("\"addr:street:rm\":\"" + streetRm + "\",");
                }
            } else {
                s.append("\"addr:place\":\"" + (street != null ? street : "") + "\",");
                if (streetDe != null) {
                    s.append("\"addr:place:de\":\"" + streetDe + "\",");
                }
                if (streetFr != null) {
                    s.append("\"addr:place:fr\":\"" + streetFr + "\",");
                }
                if (streetIt != null) {
                    s.append("\"addr:place:it\":\"" + streetIt + "\",");
                }
                if (streetRm != null) {
                    s.append("\"addr:place:rm\":\"" + streetRm + "\",");
                }
            }
            s.append("\"addr:postcode\":\"" + postcode + "\",");
            s.append("\"addr:city\":\"" + city + "\"");
            s.append("},\n");
            s.append("\"geometry\":{\"type\":\"Point\",\"coordinates\":[");
            s.append(Float.toString(lon));
            s.append(",");
            s.append(Float.toString(lat));
            s.append("]}\n");
            s.append("}");
            return s.toString();
        }
    }

    private static class Warnings implements GeoJsonOut {
        String      osmGeom;
        long        osmId;
        boolean     postcode;
        String      osmPostcode;
        String      gwrPostcode;
        boolean     city;
        String      osmCity;
        String      gwrCity;
        boolean     place;
        boolean     distance;
        boolean     noStreet;
        boolean     notOfficial;
        boolean     nonGWR;
        final float lon;
        final float lat;

        public Warnings(String osmGeom, long osmId, float lon, float lat) {
            this.osmGeom = osmGeom;
            this.osmId = osmId;
            this.lon = lon;
            this.lat = lat;
        }

        boolean hasWarning() {
            return postcode || city || distance || place || notOfficial || nonGWR;
        }

        @Override
        public String toGeoJson() {
            StringBuilder s = new StringBuilder();
            s.append("{\"type\":\"Feature\",\n");
            s.append("\"properties\":{");
            s.append("\"OSM geometry\":\"" + osmGeom + "\",");
            s.append("\"OSM id\":" + osmId);
            if (postcode) {
                s.append(",");
                s.append("\"missing or wrong addr:postcode\":\"" + postcode + "\",");
                s.append("\"OSM postcode\":\"" + osmPostcode + "\",");
                s.append("\"GWR postcode\":\"" + gwrPostcode + "\"");
            }
            if (city) {
                s.append(",");
                s.append("\"missing or wrong addr:city\":\"" + city + "\",");
                s.append("\"OSM city\":\"" + osmCity + "\",");
                s.append("\"GWR city\":\"" + gwrCity + "\"");
            }
            if (place) {
                s.append(",");
                s.append("\"addr:street instead of addr:place\":\"" + place + "\"");
            }
            if (distance) {
                s.append(",");
                s.append("\"distance more than 50 m\":\"" + distance + "\"");
            }
            if (noStreet) {
                s.append(",");
                s.append("\"no addr:street or addr:place\":\"" + noStreet + "\"");
            }
            if (notOfficial) {
                s.append(",");
                s.append("\"not official\":\"" + notOfficial + "\"");
            }
            if (nonGWR) {
                s.append(",");
                s.append("\"not in GWR\":\"" + nonGWR + "\"");
            }
            s.append("},\n");
            s.append("\"geometry\":{\"type\":\"Point\",\"coordinates\":[");
            s.append(Float.toString(lon));
            s.append(",");
            s.append(Float.toString(lat));
            s.append("]}\n");
            s.append("}");
            return s.toString();
        }
    }

    private class Stats {
        int osmBuildingAddressesCount  = 0;
        int osmNodeAddressesCount      = 0;
        int gwrAddressesCount          = 0;
        int gwrAncillaryAddressesCount = 0;
        int gwrDuplicates              = 0;
        int matchingCount              = 0;
        int matchingAncillaryCount     = 0;
        int missingCount               = 0;
        int postcodeCount              = 0;
        int cityCount                  = 0;
        int distanceCount              = 0;
        int noStreetCount              = 0;
        int notOfficialCount           = 0;
        int nonGWRCount                = 0;
        int placeCount                 = 0;
        int warningsCount              = 0;

        List<Address>  missing;
        List<Warnings> warnings;
    }

    private final Stats global = new Stats();

    private Map<String, Stats> cantonal = new HashMap<>();

    public static void main(String[] args) {

        Option outputFileOption = Option.builder("o").longOpt(OUTPUT_OPT).hasArg().desc("output html file, default: standard out").build();
        Option userOption = Option.builder("p").longOpt(USER_OPT).hasArg().desc("user, default: www-data").build();
        Option passwordOption = Option.builder("p").longOpt(PASSWORD_OPT).hasArg().desc("password, default: none").build();
        Option connectionOption = Option.builder("c").longOpt(CONNECTION_OPT).hasArg().desc("database url, default: jdbc:postgresql://localhost/gis").build();
        Option municipalityOption = Option.builder("m").longOpt(MUNICIPALITY_OPT).hasArg().desc("municiplality name, default is all municiplities").build();
        Option officialLimitOption = Option.builder("l").longOpt(OFFICIAL_VALID_LIMIT_OPT).hasArg()
                .desc("limit as a fraction of one, from which on we consider the official flag valid").build();

        Options options = new Options();

        options.addOption(outputFileOption);
        options.addOption(userOption);
        options.addOption(passwordOption);
        options.addOption(connectionOption);
        options.addOption(municipalityOption);
        options.addOption(officialLimitOption);

        CommandLineParser parser = new DefaultParser();
        try {
            // parse the command line arguments
            CommandLine line = parser.parse(options, args);
            String url = line.hasOption(CONNECTION_OPT) ? line.getOptionValue(CONNECTION_OPT) : "jdbc:postgresql://localhost/gis";
            String user = line.hasOption(USER_OPT) ? line.getOptionValue(USER_OPT) : "www-data";
            String password = line.hasOption(PASSWORD_OPT) ? line.getOptionValue(PASSWORD_OPT) : "";
            String municipality = line.hasOption(MUNICIPALITY_OPT) ? line.getOptionValue(MUNICIPALITY_OPT) : null;
            float officialValidLimit = line.hasOption(OFFICIAL_VALID_LIMIT_OPT) ? Float.parseFloat(line.getOptionValue(OFFICIAL_VALID_LIMIT_OPT))
                    : DEFAULT_OFFICIAL_VALID_LIMIT;
            try (OutputStream os = line.hasOption(OUTPUT_OPT) ? new FileOutputStream(line.getOptionValue(OUTPUT_OPT)) : System.out) {
                GWRcompare app = new GWRcompare();
                app.run(os, url, user, password, municipality, officialValidLimit);
            }
        } catch (ParseException | NumberFormatException exp) {
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp(GWRcompare.class.getSimpleName(), options);
        } catch (FileNotFoundException e) {
            System.err.println("File not found: " + e.getMessage());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void run(@NotNull OutputStream out, @NotNull String connection, @Nullable String user, @Nullable String password, @Nullable String municipality,
            float officialValidLimit) {
        Properties props = new Properties();
        props.setProperty(USER_PROP, user);
        props.setProperty(PASSWORD_PROP, password);

        File warningsDir = new File(WARNINGS_DIR);
        File missingDir = new File(MISSING_DIR);

        try (PrintWriter pw = new PrintWriter(out); Connection conn = DriverManager.getConnection(connection, props)) {

            pw.println("<H3>Updated - " + new SimpleDateFormat("yyyy-MM-dd", Locale.US).format(new Date(System.currentTimeMillis())) + "</H3>");
            pw.println("<table class=\"sortable\">");
            pw.println("<tr><th>Municipality</th><th>Canton</th><th>GWR Data</th>" + "<th class=\"sorttable_numeric\">GWR</th>"
                    + "<th class=\"sorttable_numeric\">GWR<BR>ancillary</th>" + "<th class=\"sorttable_numeric\">GWR<BR>duplicates</th>"
                    + "<th class=\"sorttable_numeric\">OSM<BR>Total</th>" + "<th class=\"sorttable_numeric\">OSM<BR>Buildings</th>"
                    + "<th class=\"sorttable_numeric\">OSM<BR>Nodes</th>" + "<th class=\"sorttable_numeric\">Matching</th>"
                    + "<th class=\"sorttable_numeric\">%<BR>Matching</th>" + "<th class=\"sorttable_numeric\">Matching<BR>ancillary</th>"
                    + "<th class=\"sorttable_numeric\">Missing</th>" + "<th class=\"sorttable_numeric\">Different or<br>missing<br>postcode</th>"
                    + "<th class=\"sorttable_numeric\">Different or<br>missing<br>city</th>"
                    + "<th class=\"sorttable_numeric\">Distance<br>more than<br>50 m</th>"
                    + "<th class=\"sorttable_numeric\">addr:street<br>instead of<br>addr:place</th>"
                    + "<th class=\"sorttable_numeric\">addr:street/<br>addr:place<br>missing</th>" + "<th class=\"sorttable_numeric\">Not official</th>"
                    + "<th class=\"sorttable_numeric\">Non-GWR</th>" + "<th class=\"sorttable_numeric\">Warnings<br>total</th></tr>");

            try (Statement stmt = conn.createStatement();
                    ResultSet municipalities = municipality != null
                            ? stmt.executeQuery("select distinct osm_id,name,muni_ref from buffered_boundaries where name='" + municipality + "'")
                            : stmt.executeQuery("select distinct osm_id,name,muni_ref from buffered_boundaries b order by name");
                    PreparedStatement gwrAddressQuery = conn.prepareStatement(
                            "select EGID, EGAID, g.ESID, g.GDENR, GDENAME, STRNAME, DEINR, PLZ4, PLZZ, PLZNAME, STRSP, strtype, gkat, gklas, doffadr, ST_X(loc), ST_Y(loc) from gwr_addresses g, planet_osm_polygon p, esid_type e "
                                    + "where p.boundary='administrative' and p.admin_level='8' and tags->'swisstopo:BFS_NUMMER'=? and e.esid=g.esid and ST_Contains(ST_Transform(p.way,4326),g.loc) "
                                    + "and g.gstat = 1004");
                    PreparedStatement osmBuildingAddressQuery = conn
                            .prepareStatement("with mp as (select ST_Multi(ST_Collect(way)) as w from planet_osm_polygon where osm_id = ?) "
                                    + "select p.osm_id as osmid,\"addr:housenumber\" as housenumber,\"addr:housename\" as housename, "
                                    + "tags->'addr:street' as street, tags->'addr:street:de' as streetde,  tags->'addr:street:fr' as streetfr, tags->'addr:street:it' as streetit, tags->'addr:street:rm' as streetrm, "
                                    + "tags->'addr:place' as aplace,  tags->'addr:place:de' as placede,  tags->'addr:place:fr' as placefr, tags->'addr:place:it' as placeit, tags->'addr:place:rm' as placerm, "
                                    + "tags->'addr:postcode' as postcode, tags->'addr:city' as city, tags->'addr:full' as afull, ST_X(ST_PointOnSurface(ST_Transform(p.way,4326))), ST_Y(ST_PointOnSurface(ST_Transform(p.way,4326))) from planet_osm_polygon p,mp "
                                    + "where ST_IsValid(p.way) AND not St_IsEmpty(p.way) AND (p.\"addr:housenumber\" is not NULL or p.\"addr:housename\" is not NULL or exist(p.tags , 'addr:full')  or  exist(p.tags , 'addr:conscriptionnumber')) AND St_IsValid(mp.w) AND St_Covers(mp.w,p.way)");
                    PreparedStatement osmBuildingAddressQuery2 = conn
                            .prepareStatement("with mp as (select ST_Multi(ST_Collect(way)) as w from planet_osm_polygon where osm_id = ?) "
                                    + "select p.osm_id as osmid,\"addr:housenumber\" as housenumber,\"addr:housename\" as housename, "
                                    + "tags->'addr:street' as street, tags->'addr:street:de' as streetde,  tags->'addr:street:fr' as streetfr, tags->'addr:street:it' as streetit, tags->'addr:street:rm' as streetrm, "
                                    + "tags->'addr:place' as aplace,  tags->'addr:place:de' as placede,  tags->'addr:place:fr' as placefr, tags->'addr:place:it' as placeit, tags->'addr:place:rm' as placerm, "
                                    + "tags->'addr:postcode' as postcode, tags->'addr:city' as city, tags->'addr:full' as afull, ST_X(ST_PointOnSurface(ST_Transform(p.way,4326))), ST_Y(ST_PointOnSurface(ST_Transform(p.way,4326))) from planet_osm_line p,mp "
                                    + "where ST_IsValid(p.way) AND not St_IsEmpty(p.way) AND (p.\"addr:housenumber\" is not NULL or p.\"addr:housename\" is not NULL or exist(p.tags , 'addr:full')  or  exist(p.tags , 'addr:conscriptionnumber')) AND St_IsValid(mp.w) AND St_Covers(mp.w,p.way)");

                    PreparedStatement osmNodeAddressQuery = conn
                            .prepareStatement("select p.osm_id as osmid,\"addr:housenumber\" as housenumber,\"addr:housename\" as housename, "
                                    + "tags->'addr:street' as street, tags->'addr:street:de' as streetde,  tags->'addr:street:fr' as streetfr, tags->'addr:street:it' as streetit, tags->'addr:street:rm' as streetrm, "
                                    + "tags->'addr:place' as aplace,  tags->'addr:place:de' as placede,  tags->'addr:place:fr' as placefr, tags->'addr:place:it' as placeit, tags->'addr:place:rm' as placerm, "
                                    + "tags->'addr:postcode' as postcode, tags->'addr:city' as city, tags->'addr:full' as afull, ST_X(ST_Transform(p.way,4326)), ST_Y(ST_Transform(p.way,4326)) from planet_osm_point p,buffered_boundaries b "
                                    + "where (p.\"addr:housenumber\" is not NULL   or p.\"addr:housename\" is not NULL  or  exist(p.tags , 'addr:full')  or  exist(p.tags , 'addr:conscriptionnumber')) AND St_IsValid(b.way) AND St_Covers(b.way,p.way) and b.osm_id=?");
                    PreparedStatement updateStats = conn.prepareStatement("update muni_address_stats set density=? where muni_ref=?");
                    PreparedStatement insertStats = conn.prepareStatement("insert into muni_address_stats (muni_ref,density) values(?,?)");
                    PreparedStatement muniCantonQuery = conn.prepareStatement("select distinct gdekt from gwr_addresses where gdenr=?");) {
                // loop over municipalities
                while (municipalities.next()) {
                    long muniBoundaryId = municipalities.getInt(1);
                    String muniName = municipalities.getString(2);
                    String muniRef = municipalities.getString(3);

                    // get canton
                    muniCantonQuery.setInt(1, Integer.parseInt(muniRef));
                    ResultSet canton = muniCantonQuery.executeQuery();
                    String muniCanton = "?";
                    if (canton.next()) {
                        muniCanton = canton.getString(1);
                    }

                    // get GWR addresses
                    MultiHashMap<String, Address> gwrAddressesMap = new MultiHashMap<>();
                    Map<String, Boolean> gwrHasValidation = new HashMap<>();
                    Map<Long, Address> seen = new HashMap<>();
                    gwrAddressQuery.setString(1, muniRef);
                    ResultSet gwrAddresses = gwrAddressQuery.executeQuery();
                    int gwrCount = 0;
                    int gwrAncillaryCount = 0;
                    int gwrNoNumber = 0;
                    int officialCount = 0;
                    while (gwrAddresses.next()) {
                        long addressId = gwrAddresses.getLong(2);
                        Address seenAddress = seen.get(addressId);
                        if (seenAddress != null) {
                            // multilingual
                            if (seenAddress.street != null) {
                                // move to correct language
                                switch (seenAddress.streetLang) {
                                case LANG_DE:
                                    seenAddress.streetDe = seenAddress.street;
                                    break;
                                case LANG_RM:
                                    seenAddress.streetRm = seenAddress.street;
                                    break;
                                case LANG_FR:
                                    seenAddress.streetFr = seenAddress.street;
                                    break;
                                case LANG_IT:
                                    seenAddress.streetIt = seenAddress.street;
                                    break;
                                default:
                                    // no language set
                                }
                                seenAddress.street = null;
                            }
                            // add street name to correct field
                            String street = gwrAddresses.getString(6);
                            switch (gwrAddresses.getString(11)) {
                            case GWR_LANG_DE:
                                seenAddress.streetDe = street;
                                break;
                            case GWR_LANG_RM:
                                seenAddress.streetRm = street;
                                break;
                            case GWR_LANG_FR:
                                seenAddress.streetFr = street;
                                break;
                            case GWR_LANG_IT:
                                seenAddress.streetIt = street;
                                break;
                            default:
                                // no language set
                            }
                            continue;
                        }
                        Address address = new Address();
                        address.housenumber = gwrAddresses.getString(7);
                        if (address.housenumber == null) {
                            gwrNoNumber++;
                            continue;
                        }
                        address.street = gwrAddresses.getString(6);
                        address.streetType = gwrAddresses.getString(12);
                        switch (gwrAddresses.getString(11)) {
                        case GWR_LANG_DE:
                            address.streetLang = LANG_DE;
                            break;
                        case GWR_LANG_RM:
                            address.streetLang = LANG_RM;
                            break;
                        case GWR_LANG_FR:
                            address.streetLang = LANG_FR;
                            break;
                        case GWR_LANG_IT:
                            address.streetLang = LANG_IT;
                            break;
                        default:
                            // no language set
                        }
                        address.postcode = gwrAddresses.getString(8);
                        address.city = gwrAddresses.getString(10);
                        address.gwrCategory = gwrAddresses.getInt(13);
                        address.gwrClass = gwrAddresses.getInt(14);
                        address.official = gwrAddresses.getBoolean(15);
                        if (address.official) {
                            officialCount++;
                        }
                        address.lon = gwrAddresses.getFloat(16);
                        address.lat = gwrAddresses.getFloat(17);
                        if (!address.isAncillary()) {
                            gwrCount++;
                        } else {
                            gwrAncillaryCount++;
                        }
                        gwrAddressesMap.add(createKey(address.street, address.housenumber), address);
                        seen.put(addressId, address);
                    }
                    // if more than OFFICIAL_VALID_LIMIT of the addresses have the official flag set assume that the
                    // flag is valid
                    if (gwrCount > 0 && officialCount / gwrCount >= officialValidLimit) {
                        gwrHasValidation.put(muniRef, true);
                    }
                    global.gwrAddressesCount += gwrCount;
                    global.gwrAncillaryAddressesCount += gwrAncillaryCount;

                    // accumulate per canton stats
                    Stats cantonalStats = cantonal.get(muniCanton);
                    if (cantonalStats == null) {
                        cantonalStats = new Stats();
                        cantonal.put(muniCanton, cantonalStats);
                    }
                    cantonalStats.gwrAddressesCount += gwrCount;
                    cantonalStats.gwrAncillaryAddressesCount += gwrAncillaryCount;

                    MultiHashMap<String, Address> osmAddresses = new MultiHashMap<>();

                    // get OSM addresses
                    osmBuildingAddressQuery.setLong(1, muniBoundaryId);
                    ResultSet osmBuildingAddresses = osmBuildingAddressQuery.executeQuery();
                    int osmBuildingsCount = getOsmAddresses("polygon", osmAddresses, osmBuildingAddresses, gwrAddressesMap);
                    osmBuildingAddressQuery2.setLong(1, muniBoundaryId);
                    osmBuildingAddresses = osmBuildingAddressQuery2.executeQuery();
                    osmBuildingsCount += getOsmAddresses("polygon", osmAddresses, osmBuildingAddresses, gwrAddressesMap);
                    global.osmBuildingAddressesCount += osmBuildingsCount;
                    cantonalStats.osmBuildingAddressesCount += osmBuildingsCount;

                    osmNodeAddressQuery.setLong(1, muniBoundaryId);
                    ResultSet osmNodeAddresses = osmNodeAddressQuery.executeQuery();
                    int osmNodesCount = getOsmAddresses("point", osmAddresses, osmNodeAddresses, gwrAddressesMap);
                    global.osmNodeAddressesCount += osmNodesCount;
                    cantonalStats.osmNodeAddressesCount += osmNodesCount;

                    //
                    int notOfficial = 0;
                    int gwrDuplicates = 0;
                    //
                    List<Address> matching = new ArrayList<>();
                    List<Address> matchingAncillary = new ArrayList<>();
                    List<Address> missing = new ArrayList<>();
                    List<Address> postcode = new ArrayList<>();
                    List<Address> city = new ArrayList<>();
                    List<Address> distance = new ArrayList<>();
                    List<Address> place = new ArrayList<>();
                    List<Warnings> warnings = new ArrayList<>();
                    for (String k : new ArrayList<>(gwrAddressesMap.getKeys())) {
                        List<Address> sameKey = new ArrayList<>(gwrAddressesMap.get(k));
                        // check for duplicates
                        // for now we count them and then remove all but one
                        if (sameKey.size() > 1) {
                            MultiHashMap<String, Address> samePostcode = new MultiHashMap<>();
                            for (Address a : sameKey) {
                                samePostcode.add(a.postcode, a);
                            }
                            for (String p : samePostcode.getKeys()) {
                                List<Address> dups = new ArrayList<>(samePostcode.get(p));
                                if (dups.size() > 1) {
                                    gwrDuplicates += dups.size() - 1;
                                    for (int i = 1; i < dups.size(); i++) {
                                        gwrAddressesMap.removeItem(k, dups.get(i));
                                    }
                                }
                            }
                        }
                        for (Address gwr : new ArrayList<>(gwrAddressesMap.get(k))) {
                            Address osm = null;
                            String key = null;
                            if (gwr.street == null) { // multilingual
                                for (String street : new String[] { gwr.streetDe, gwr.streetRm, gwr.streetFr, gwr.streetIt }) {
                                    if (street != null) {
                                        key = createKey(street, gwr.housenumber);
                                        Set<Address> temp = osmAddresses.get(key);
                                        if (!temp.isEmpty()) {
                                            osm = temp.iterator().next();
                                            break;
                                        }
                                    }
                                }
                            } else {
                                key = createKey(gwr.street, gwr.housenumber);
                                Set<Address> temp = osmAddresses.get(key);
                                if (!temp.isEmpty()) {
                                    double lowestDistance = Double.MAX_VALUE;
                                    Address closest = null;
                                    for (Address o : temp) {
                                        double tempDistance = haversineDistance(gwr.lon, gwr.lat, o.lon, o.lat);
                                        if (tempDistance < lowestDistance) {
                                            closest = o;
                                            lowestDistance = tempDistance;
                                        }
                                    }
                                    if (gwr.postcode.equals(closest.postcode) || lowestDistance <= 50) {
                                        osm = closest;
                                    }
                                }
                            }
                            final boolean ancillary = gwr.isAncillary();
                            if (osm != null) {
                                for (Address a : new ArrayList<>(osmAddresses.get(key))) {
                                    // skip addresses that would not have matched above
                                    final boolean noPostCodeMatch = !gwr.postcode.equals(a.postcode);
                                    double tempDistance = haversineDistance(gwr.lon, gwr.lat, a.lon, a.lat);

                                    if (noPostCodeMatch && tempDistance > 50) {
                                        continue;
                                    }

                                    Warnings w = new Warnings(a.osmGeom, a.osmId, a.lon, a.lat);

                                    if (noPostCodeMatch) {
                                        postcode.add(a);
                                        w.postcode = true;
                                        w.osmPostcode = a.postcode;
                                        w.gwrPostcode = gwr.postcode;
                                    }
                                    if (!gwr.city.equals(a.city)) {
                                        city.add(a);
                                        w.city = true;
                                        w.osmCity = a.city;
                                        w.gwrCity = gwr.city;
                                    }
                                    if (tempDistance > MATCHING_DISTANCE) {
                                        distance.add(a);
                                        w.distance = true;
                                    }
                                    if (!SWISSTOPO_STREET_GEOM.equals(gwr.streetType) && a.place == null) {
                                        place.add(a);
                                        w.place = true;
                                    }
                                    w.notOfficial = !gwr.official;
                                    if (w.notOfficial && !ancillary) {
                                        notOfficial++;
                                    }
                                    if (w.hasWarning()) {
                                        warnings.add(w);
                                    }
                                    osmAddresses.removeItem(key, a);
                                }
                                if (ancillary) {
                                    matchingAncillary.add(osm);
                                } else {
                                    matching.add(osm);
                                }
                                gwrAddressesMap.removeItem(key, gwr);
                                continue;
                            }
                            if (!ancillary && (gwr.official || !gwrHasValidation.containsKey(muniRef))) {
                                missing.add(gwr);
                            }
                        }
                    }
                    int noStreet = 0;
                    for (Address leftOver : osmAddresses.getValues()) {
                        Warnings w = new Warnings(leftOver.osmGeom, leftOver.osmId, leftOver.lon, leftOver.lat);
                        if (leftOver.street == null && leftOver.place == null) {
                            w.noStreet = true;
                            noStreet++;
                        } else {
                            w.nonGWR = true;
                        }
                        warnings.add(w);
                    }
                    final int osmMatching = matching.size();
                    global.matchingCount += osmMatching;
                    global.matchingAncillaryCount += matchingAncillary.size();
                    global.missingCount += missing.size();
                    global.postcodeCount += postcode.size();
                    global.cityCount += city.size();
                    global.distanceCount += distance.size();
                    global.placeCount += place.size();
                    global.noStreetCount += noStreet;
                    global.notOfficialCount += notOfficial;
                    global.nonGWRCount += osmAddresses.size() - noStreet;
                    global.warningsCount += warnings.size();
                    global.gwrDuplicates += gwrDuplicates;

                    cantonalStats.matchingCount += osmMatching;
                    cantonalStats.matchingAncillaryCount += matchingAncillary.size();
                    cantonalStats.missingCount += missing.size();
                    cantonalStats.postcodeCount += postcode.size();
                    cantonalStats.cityCount += city.size();
                    cantonalStats.distanceCount += distance.size();
                    cantonalStats.placeCount += place.size();
                    cantonalStats.noStreetCount += noStreet;
                    cantonalStats.notOfficialCount += notOfficial;
                    cantonalStats.nonGWRCount += osmAddresses.size() - noStreet;
                    cantonalStats.warningsCount += warnings.size();
                    cantonalStats.gwrDuplicates += gwrDuplicates;

                    pw.print("<tr><td>" + muniName + "</td><td>" + muniCanton + "</td><td align=\"center\">" + "<a href=\"https://qa.poole.ch/addresses/GWR/"
                            + muniRef + ".zip\">S</a> <a href=\"https://qa.poole.ch/addresses/GWR/" + muniRef
                            + ".geojson.zip\">G</a> <a href=\"https://qa.poole.ch/addresses/GWR/" + muniRef
                            + ".osm.zip\">O</a> <a href=\"https://qa.poole.ch/addresses/GWR/" + muniRef
                            + "_all.geojson.zip\">GA</a> <a href=\"https://qa.poole.ch/addresses/GWR/" + muniRef + "_all.osm.zip\">OA</a></td>");
                    pw.print("<td align=\"right\">" + gwrCount + "</td>");
                    pw.print("<td align=\"right\">" + gwrAncillaryCount + "</td>");
                    pw.print("<td align=\"right\">" + gwrDuplicates + "</td>");

                    final int osmTotal = osmBuildingsCount + osmNodesCount;

                    pw.println("<td align=\"right\">" + osmTotal + "</td><td align=\"right\">" + osmBuildingsCount + "</td><td align=\"right\">" + osmNodesCount
                            + "</td>");
                    File warningsFile = new File(warningsDir, muniRef + ".geojson");
                    File missingFile = new File(missingDir, muniRef + ".geojson");
                    pw.println("<td align=\"right\">" + osmMatching + "</td>");

                    if (gwrCount != 0) {
                        pw.printf("<td align=\"right\">%1$d</td>", (int) (osmMatching * 100f / (gwrCount - gwrDuplicates)));
                    } else {
                        pw.print("<td align=\"right\">-</td>");
                    }

                    pw.println("<td align=\"right\">" + matchingAncillary.size() + "</td><td align=\"right\">" + "<a href=\"https://qa.poole.ch/addresses/ch/"
                            + missingFile.getPath() + "\" download=\"missing-" + muniRef + ".geojson\">" + missing.size() + "</a></td><td align=\"right\">"
                            + postcode.size() + "</td><td align=\"right\">" + city.size() + "</td><td align=\"right\">" + distance.size()
                            + "</td><td align=\"right\">" + place.size() + "</td><td align=\"right\">" + noStreet + "</td><td align=\"right\">" + notOfficial
                            + "</td><td align=\"right\">" + (osmAddresses.size() - noStreet) + "</td><td align=\"right\">"
                            + "<a href=\"https://qa.poole.ch/addresses/ch/" + warningsFile.getPath() + "\" download=\"warnings-" + muniRef + ".geojson\">"
                            + warnings.size() + "</a></td></tr>");

                    writeGeoJsonListToFile(warnings, warningsFile);
                    writeGeoJsonListToFile(missing, missingFile);

                    if (cantonalStats.warnings == null) {
                        cantonalStats.warnings = new ArrayList<>();
                    }
                    cantonalStats.warnings.addAll(warnings);

                    if (cantonalStats.missing == null) {
                        cantonalStats.missing = new ArrayList<>();
                    }
                    cantonalStats.missing.addAll(missing);

                    if (gwrCount != 0) {
                        double density = osmMatching / (double) gwrCount;
                        long muniRefLong = Long.parseLong(muniRef);
                        updateStats.setDouble(1, density);
                        updateStats.setLong(2, muniRefLong);
                        try {
                            int rows = updateStats.executeUpdate();
                            if (rows == 0) {
                                throw new SQLException("stats row doesn't exist");
                            }
                        } catch (SQLException ex) {
                            insertStats.setLong(1, muniRefLong);
                            insertStats.setDouble(2, density);
                            insertStats.executeUpdate();
                        }
                    }
                }
                pw.println("<tr class=\"sortbottom\">");
                printStatsLine(pw, "TOTAL", global, true);
                pw.println("</table>");

                pw.println("<H4>Cantons</H4>");
                pw.println("<table class=\"sortable\">");
                pw.println("<tr><th>Canton</th><th class=\"sorttable_numeric\">GWR</th>" + "<th class=\"sorttable_numeric\">GWR<BR>ancillary</th>"
                        + "<th class=\"sorttable_numeric\">GWR<BR>duplicates</th>" + "<th class=\"sorttable_numeric\">OSM<BR>Total</th>"
                        + "<th class=\"sorttable_numeric\">OSM<BR>Buildings</th>" + "<th class=\"sorttable_numeric\">OSM<BR>Nodes</th>"
                        + "<th class=\"sorttable_numeric\">Matching</th>" + "<th class=\"sorttable_numeric\">%<BR>Matching</th>"
                        + "<th class=\"sorttable_numeric\">Matching<BR>ancillary</th>" + "<th class=\"sorttable_numeric\">Missing</th>"
                        + "<th class=\"sorttable_numeric\">Different or<br>missing<br>postcode</th>"
                        + "<th class=\"sorttable_numeric\">Different or<br>missing<br>city</th>"
                        + "<th class=\"sorttable_numeric\">Distance<br>more than<br>50 m</th>"
                        + "<th class=\"sorttable_numeric\">addr:street<br>instead of<br>addr:place</th>"
                        + "<th class=\"sorttable_numeric\">addr:street/<br>addr:place<br>missing</th>" + "<th class=\"sorttable_numeric\">Not official</th>"
                        + "<th class=\"sorttable_numeric\">Non-GWR</th>" + "<th class=\"sorttable_numeric\">Warnings<br>total</th></tr>");

                List<String> cantonsList = new ArrayList<>(cantonal.keySet());
                Collections.sort(cantonsList);
                for (String canton : cantonsList) {
                    Stats cantonalStats = cantonal.get(canton);
                    pw.println("<tr>");
                    printStatsLine(pw, canton, cantonalStats, false);
                    writeGeoJsonListToFile(cantonalStats.warnings, new File(warningsDir, canton + ".geojson"));
                    writeGeoJsonListToFile(cantonalStats.missing, new File(missingDir, canton + ".geojson"));
                }
                pw.println("</table>");
            }
        } catch (FileNotFoundException | SQLException e) {
            e.printStackTrace();
        }
    }

    /**
     * Print out one line of stats, without the start or the
     * <tr>
     * element
     * 
     * @param pw the PrintWriter
     * @param name name of the line
     * @param stats the Stats object
     * @param fullGwrData if false suppress some fields
     */
    private void printStatsLine(@NotNull PrintWriter pw, @NotNull String name, @NotNull Stats stats, boolean fullGwrData) {
        pw.print("<td><b>" + name + "</b></td>");
        if (fullGwrData) {
            pw.print("<td></td><td></td>");
        }
        pw.println("<td align=\"right\"><b>" + stats.gwrAddressesCount + "</b></td><td align=\"right\">" + stats.gwrAncillaryAddressesCount
                + "</td><td align=\"right\">" + stats.gwrDuplicates + "</td><td align=\"right\"><b>"
                + (stats.osmBuildingAddressesCount + stats.osmNodeAddressesCount) + "</b></td><td align=\"right\"><b>" + stats.osmBuildingAddressesCount
                + "</b></td><td align=\"right\"><b>" + stats.osmNodeAddressesCount + "</b></td>");
        pw.println("<td align=\"right\"><b>" + stats.matchingCount + "</b></td>");
        if (stats.gwrAddressesCount != 0) {
            pw.printf("<td align=\"right\">%1$d</td>", (int) (stats.matchingCount * 100f / (stats.gwrAddressesCount - stats.gwrDuplicates)));
        } else {
            pw.print("<td align=\"right\">-</td>");
        }
        pw.println("<td align=\"right\">" + stats.matchingAncillaryCount + "</td><td align=\"right\"><b>" + stats.missingCount + "</b></td><td align=\"right\">"
                + stats.postcodeCount + "</td><td align=\"right\">" + stats.cityCount + "</td><td align=\"right\">" + stats.distanceCount
                + "</td><td align=\"right\">" + stats.placeCount + "</td><td align=\"right\">" + stats.noStreetCount + "</td><td align=\"right\">"
                + stats.notOfficialCount + "</td><td align=\"right\">" + stats.nonGWRCount + "</td><td align=\"right\"><b>" + stats.warningsCount
                + "</b></td></tr>");
    }

    /**
     * Write a list of objects to a GeoJson FeatureCollection in a file
     * 
     * @param list the list of objects
     * @param file the File
     * @throws FileNotFoundException
     */
    private <T extends GeoJsonOut> void writeGeoJsonListToFile(@NotNull List<T> list, @NotNull File file) throws FileNotFoundException {
        try (PrintWriter writer = new PrintWriter(new FileOutputStream(file))) {
            writer.println("{\"type\":\"FeatureCollection\",");
            writer.println("\"features\":[");
            boolean first = true;
            for (T w : list) {
                if (first) {
                    first = false;
                } else {
                    writer.println(",");
                }
                writer.println(w.toGeoJson());
            }
            writer.println("]}");
        }
    }

    /**
     * @param osmGeom an indication of if this is for a polygon or a point
     * @param osmAddresses a Map that will contain the osm addresses
     * @param addresses the ResultSet from the database
     * @param gwrAddressesMap the GWR addresses for the municipality
     * @return a count of addresses
     * @throws SQLException
     */
    private static int getOsmAddresses(String osmGeom, MultiHashMap<String, Address> osmAddresses, ResultSet addresses,
            MultiHashMap<String, Address> gwrAddressesMap) throws SQLException {
        int count = 0;
        while (addresses.next()) {
            count++;
            String housenumber = addresses.getString(2);
            if (housenumber == null) {
                Address address = new Address();
                addNonNumberFields(osmGeom, addresses, address, gwrAddressesMap);
                osmAddresses.add(createKey(address.street != null ? address.street : address.place, address.housename), address);
                continue;
            }
            String[] numbers = housenumber.split("[;,]");
            for (String number : numbers) {
                Address address = new Address();
                address.housenumber = number.replaceAll("\\s", "");
                addNonNumberFields(osmGeom, addresses, address, gwrAddressesMap);
                osmAddresses.add(createKey(address.street != null ? address.street : address.place, address.housenumber), address);
            }
        }
        return count;
    }

    /**
     * Add all non-housenumber fields
     * 
     * @param osmGeom the OSM geometry
     * @param addresses ResultSet with OSM addresses from query
     * @param address Address object
     * @param gwrAddressesMap the GWR addresses
     * @throws SQLException
     */
    private static void addNonNumberFields(@NotNull String osmGeom, @NotNull ResultSet addresses, @NotNull Address address,
            @NotNull MultiHashMap<String, Address> gwrAddressesMap) throws SQLException {
        address.osmGeom = osmGeom;
        address.osmId = addresses.getLong(1);
        address.housename = addresses.getString(3);
        String street = addresses.getString(4);
        String streetde = addresses.getString(5);
        String streetfr = addresses.getString(6);
        String streetit = addresses.getString(7);
        String streetrm = addresses.getString(8);
        String place = addresses.getString(9);
        String placede = addresses.getString(10);
        String placefr = addresses.getString(11);
        String placeit = addresses.getString(12);
        String placerm = addresses.getString(13);
        // this is a hack to determine if we need to use a multi-lingual street / place name
        // useful for example for Biel/Bienne
        address.street = street;
        if (streetde != null && gwrAddressesMap.containsKey(createKey(streetde, address.housenumber))) {
            address.street = streetde;
            address.streetLang = LANG_DE;
        } else if (streetfr != null && gwrAddressesMap.containsKey(createKey(streetfr, address.housenumber))) {
            address.street = streetfr;
            address.streetLang = LANG_FR;
        } else if (streetit != null && gwrAddressesMap.containsKey(createKey(streetit, address.housenumber))) {
            address.street = streetit;
            address.streetLang = LANG_IT;
        } else if (streetrm != null && gwrAddressesMap.containsKey(createKey(streetrm, address.housenumber))) {
            address.street = streetrm;
            address.streetLang = LANG_RM;
        }
        address.place = place;
        if (placede != null && gwrAddressesMap.containsKey(createKey(placede, address.housenumber))) {
            address.place = placede;
            address.streetLang = LANG_DE;
        } else if (placefr != null && gwrAddressesMap.containsKey(createKey(placefr, address.housenumber))) {
            address.place = placefr;
            address.streetLang = LANG_FR;
        } else if (placeit != null && gwrAddressesMap.containsKey(createKey(placeit, address.housenumber))) {
            address.place = placeit;
            address.streetLang = LANG_IT;
        } else if (placerm != null && gwrAddressesMap.containsKey(createKey(placerm, address.housenumber))) {
            address.place = placerm;
            address.streetLang = LANG_RM;
        }

        address.postcode = addresses.getString(14);
        address.city = addresses.getString(15);
        address.full = addresses.getString(16);
        address.lon = addresses.getFloat(17);
        address.lat = addresses.getFloat(18);
    }

    /**
     * Create the key used for matching
     * 
     * @param name the street/place name
     * @param number the house number
     * @return a suitable key
     */
    @NotNull
    private static String createKey(@Nullable String name, @Nullable String number) {
        return (name + " " + number).toLowerCase();
    }

    /**
     * Calculate the haversine distance between two points
     * 
     * @param lon1 longitude of the first point in degrees
     * @param lat1 latitude of the first point in degree
     * @param lon2 longitude of the second point in degrees
     * @param lat2 latitude of the second point in degree
     * @return distance between the two point in meters
     */
    private static double haversineDistance(double lon1, double lat1, double lon2, double lat2) {
        double dLat = Math.toRadians(lat2 - lat1);
        double dLon = Math.toRadians(lon2 - lon1);
        lat1 = Math.toRadians(lat1);
        lat2 = Math.toRadians(lat2);

        double a = Math.sin(dLat / 2) * Math.sin(dLat / 2) + Math.sin(dLon / 2) * Math.sin(dLon / 2) * Math.cos(lat1) * Math.cos(lat2);
        double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
        return EARTH_RADIUS * c;
    }
}
