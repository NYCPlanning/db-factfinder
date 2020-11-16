CREATE TEMP TABLE tmp (
    geotype text,
    geogname text,
    geoid text,
    dataset text,
    variable text,
    c numeric,
    e numeric,
    m numeric,
    p numeric,
    z numeric,
    domain text
);

\COPY tmp FROM PSTDIN DELIMITER ',' CSV HEADER;

SELECT geotype,
        geogname,
        geoid,
        dataset,
        variable,
        c,
        e,
        m,
        p,
        z
INTO pff_demographic.:"V_PFF" 
FROM tmp
WHERE domain = 'demographic';

SELECT geotype,
        geogname,
        geoid,
        dataset,
        variable,
        c,
        e,
        m,
        p,
        z
INTO pff_economic.:"V_PFF" 
FROM tmp
WHERE domain = 'economic';

SELECT geotype,
        geogname,
        geoid,
        dataset,
        variable,
        c,
        e,
        m,
        p,
        z
INTO pff_housing.:"V_PFF" 
FROM tmp
WHERE domain = 'housing';

SELECT geotype,
        geogname,
        geoid,
        dataset,
        variable,
        c,
        e,
        m,
        p,
        z
INTO pff_social.:"V_PFF" 
FROM tmp
WHERE domain = 'social';