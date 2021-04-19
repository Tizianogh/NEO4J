CREATE CONSTRAINT ON (c:Commune) ASSERT c.ci IS UNIQUE;
CREATE CONSTRAINT ON (d:Departement) ASSERT d.cd IS UNIQUE;
CREATE CONSTRAINT ON (r:Region) ASSERT r.cr IS UNIQUE;

LOAD CSV WITH HEADERS FROM 
"file:///communes-departement-region.csv" AS row FIELDTERMINATOR ','
MERGE( c:Commune{
    ci:(CASE size(toString(row.code_commune_INSEE)) WHEN 4 THEN "0"+toString(row.code_commune_INSEE) ELSE toString(row.code_commune_INSEE) END),
    nom_commune_postal: toUpper(toString(row.nom_commune_postal))
    })
MERGE( cp:Cp{
    cp: (CASE size(toString(row.code_postal)) WHEN 4 THEN "0"+toString(row.code_postal) ELSE toString(row.code_postal) END)
})
MERGE( d:Departement{
    cd: toString((case row.code_departement when null then "-1" else row.code_departement end)),
    nom_departement: toUpper(toString((case row.nom_departement when null then "autre" else row.nom_departement end))),
    cr: toString((case row.code_region when null then "-2" else row.code_region end))
})
MERGE( r:Region{
    cr: toString((case row.code_region when null then "-2" else row.code_region end)),
    Nom_Region: toString((case row.nom_region when null then "Autre" else row.nom_region end))
}) 
CREATE (c)-[:HAS_CP]->(cp)
CREATE (c)-[:AT_DEPARTEMENT]->(d);

MATCH (d:Departement), (r:Region)
WHERE d.cr=r.cr
CREATE (d)-[:AT_REGION]->(r);

LOAD CSV WITH HEADERS FROM 
"file:///accidents.csv" AS row FIELDTERMINATOR ';'
MERGE( a:Accident{
    uuid:"",
    num_acc: toString(row.Num_Acc)
})
MERGE( d:Date{
    uuid:"",
    jour: toString(row.jour),
    mois: toString(row.mois),
    an: toString(row.an),
    hrmn: toString(row.hrmn)
})
MERGE( l:Lieu{
    uuid:"",
    ci: (CASE size(toString(row.com)) WHEN 4 THEN "0"+toString(row.com) ELSE toString(row.com) END),
    lat: toFloat(replace(toString(row.lat),',','.')),
    long: toFloat(replace(toString(row.long),',','.'))
})
MERGE( ca:CAccident{
    uuid:"",
    lum: toString(row.lum),
    int: toString(row.int),
    atm: toString(row.atm),
    col: toString(row.col)
})
CREATE(a)-[:TOOK_PLACE]->(l)
CREATE(a)-[:AT_DATE]->(d)
CREATE(a)-[:CARACTERISTIQUES]->(ca);

MATCH (l:Lieu),(c:Commune) WHERE l.ci=c.ci 
CREATE  (l)-[:ACCIDENT_IN_COMMUNE]->(c);

MATCH(a:Accident)
SET a.uuid=apoc.create.uuid();
CREATE CONSTRAINT ON (a:Accident) ASSERT a.uuid IS UNIQUE;

MATCH(d:Date)
SET d.uuid=apoc.create.uuid();
CREATE CONSTRAINT ON (d:Date) ASSERT d.uuid IS UNIQUE;

MATCH(l:Lieu)
SET l.uuid=apoc.create.uuid();
CREATE CONSTRAINT ON (l:Lieu) ASSERT l.uuid IS UNIQUE;

MATCH(ca:CAccident)
SET ca.uuid=apoc.create.uuid();
CREATE CONSTRAINT ON (ca:CAccident) ASSERT ca.uuid IS UNIQUE;

LOAD CSV WITH HEADERS FROM 
"file:///caract_lieux.csv" AS row FIELDTERMINATOR ';'
MERGE( cl:CLieu{
    uuid:"",
    num_acc: toString(row.Num_Acc),
    catr: toString(row.catr),
    voie: toString((case row.voie when null then "" else row.voie end)),
    v1: toString((case row.v1 when null then "" else row.v1 end)),
    v2: toString((case row.v2 when null then "" else row.v2 end)),
    circ: toString((case row.circ when null then "" else row.circ end)),
    nbv: toString((case row.nbv when null then "" else row.nbv end)),
    vosp: toString((case row.vosp when null then "" else row.vosp end)),
    prof: toString((case row.prof when null then "" else row.prof end)),
    pr: toString((case row.pr when null then "" else row.pr end)),
    pr1: toString((case row.pr1 when null then "" else row.pr1 end)),
    plan: toString((case row.plan when null then "" else row.plan end)),
    lartpc: toString((case row.lartpc when null then "" else row.lartpc end)),
    larrout: toString((case row.larrout when null then "" else row.larrout end)),
    surf: toString((case row.surf when null then "" else row.surf end)),
    infra: toString((case row.infra when null then "" else row.infra end)),
    situ: toString((case row.situ when null then "" else row.situ end)),
    vma: toString((case row.vma when null then "" else row.vma end))
});

MATCH(cl:CLieu)
SET cl.uuid=apoc.create.uuid();
CREATE CONSTRAINT ON (cl:CLieu) ASSERT cl.uuid IS UNIQUE;

MATCH (a:Accident)-[:TOOK_PLACE]->(l:Lieu), (cl:CLieu) WHERE a.num_acc=cl.num_acc
CREATE (l)-[:CARACTERISTIQUES_LIEU]->(cl); 


LOAD CSV WITH HEADERS FROM 
"file:///vehicules.csv" AS row FIELDTERMINATOR ';'
MERGE( v:Vehicule{
    uuid:"",
    num_acc: toString(row.Num_Acc),
    id_vehicule: toString(row.id_vehicule),
    num_veh: toString(row.num_veh),
    senc: toString(row.senc),
    catv: toString(row.catv),
    obs: toString(row.obs),
    obsm: toString(row.obsm),
    choc: toString(row.choc),
    manv: toString(row.manv),
    motor: toString(row.motor),
    occutc: toString((case row.occutc when null then "" else row.occutc end))
});

MATCH(v:Vehicule)
SET v.uuid=apoc.create.uuid();
CREATE CONSTRAINT ON (v:Vehicule) ASSERT v.uuid IS UNIQUE;

MATCH (a:Accident),(v:Vehicule) WHERE a.num_acc=v.num_acc
CREATE  (a)-[:VEHICULE_CONCERNED]->(v);

LOAD CSV WITH HEADERS FROM 
"file:///usagers.csv" AS row FIELDTERMINATOR ';'
MERGE( u:Usager{
    uuid:"",
    num_acc: toString(row.Num_Acc),
    id_vehicule: toString(row.id_vehicule),
    num_veh: toString(row.num_veh),
    place: toString(row.place),
    catu: toString(row.catu),
    grav: toString(row.grav),
    sexe: toString(row.sexe),
    an_nais: toFloat(row.an_nais),
    trajet: toString(row.trajet),
    secu1: toString(row.secu1),
    secu2: toString(row.secu2),
    secu3: toString(row.secu3),
    locp: toString(row.locp),
    actp: toString(row.actp),
    etatp: toString(row.etatp)
});

MATCH(u:Usager)
SET u.uuid=apoc.create.uuid();
CREATE CONSTRAINT ON (u:Usager) ASSERT u.uuid IS UNIQUE;

MATCH (a:Accident),(u:Usager) WHERE a.num_acc=u.num_acc
CREATE  (a)-[:USAGER_CONCERNED]->(u);


LOAD CSV WITH HEADERS FROM 
"file:///etablissementsV.csv" AS row FIELDTERMINATOR ','
MERGE( ev:EtablissementV{
    uuid: "",
    noFinesset : toString(row.noFinesset),
    title : toString(row.title),
    capacity : toFloat(row.capacity),
    cp: (CASE size(toString(row.postcode)) WHEN 4 THEN "0"+toString(row.postcode) ELSE toString(row.postcode) END),
    nom_com : toUpper(toString(row.city))
});

MATCH( ev:EtablissementV)
SET ev.uuid=apoc.create.uuid();
CREATE CONSTRAINT ON ( ev:EtablissementV) ASSERT ev.uuid IS UNIQUE;

MATCH (ev:EtablissementV),(c:Commune)-[:HAS_CP]->(cp:Cp) WHERE ev.nom_com=c.nom_commune_postal
and ev.cp=cp.cp
create (ev)-[:ETABLISSEMENTS_IN_COMMUNE]->(c);

LOAD CSV WITH HEADERS FROM 
"file:///population.csv" AS row FIELDTERMINATOR ';'
WITH row WHERE NOT row.population_totale IS NULL
WITH row WHERE NOT row.pop_60ansetplus IS NULL
MERGE( p:Population{
    ci: (CASE size(toString(row.codgeo)) WHEN 4 THEN "0"+toString(row.codgeo) ELSE toString(row.codgeo) END),
    population_totale: toFloat(row.population_totale),
    pop_60ansetplus: toFloat(row.pop_60ansetplus)
});

MATCH (p:Population),(c:Commune) WHERE  p.ci=c.ci
CREATE  (c)-[:COMMUNE_POPULATION]->(p);


LOAD CSV WITH HEADERS FROM 
"file:///bornes-irve-20191220.csv" AS row FIELDTERMINATOR ';'
WITH row WHERE apoc.meta.type(toFloat(toString(row.Ylatitude))) = "FLOAT"
WITH row WHERE apoc.meta.type(toFloat(toString(row.Xlongitude))) = "FLOAT"
MERGE( b:Borne{
    uuid:"",
    ad_station: toString(row.ad_station),
    ci: (CASE size(toString(row.code_insee)) WHEN 4 THEN "0"+toString(row.code_insee) ELSE toString(row.code_insee) END),
    lat: toFloat(row.Ylatitude),
    long: toFloat(row.Xlongitude)
});

MATCH(b:Borne)
SET b.uuid=apoc.create.uuid();
CREATE CONSTRAINT ON (b:Borne) ASSERT b.uuid IS UNIQUE;

MATCH (b:Borne),(c:Commune) WHERE  b.ci=c.ci
CREATE  (b)-[:BORNE_IN_COMMUNE]->(c);



LOAD CSV WITH HEADERS FROM 
"file:///panorama-des-festivals.csv" AS row FIELDTERMINATOR ';'
WITH row WHERE NOT row.coordonnees_insee IS NULL
WITH row WHERE NOT row.date_debut_ancien IS NULL
WITH row WHERE NOT row.date_de_fin_ancien IS NULL
WITH row WHERE NOT row.departement IS NULL
MERGE ( f:Festival{
    uuid:"",
    nom_de_la_manifestation: toString(row.nom_de_la_manifestation),
    domaine: toString(row.domaine),
    lat: toFloat(split(row.coordonnees_insee,",")[0]),
    long: toFloat(split(row.coordonnees_insee,",")[1]),
    date_debut_ancien: toString(row.date_debut_ancien),
    date_de_fin_ancien: toString(row.date_de_fin_ancien),
    cd: toString(row.departement)
});

MATCH(f:Festival)
SET f.uuid=apoc.create.uuid();

CREATE CONSTRAINT ON (f:Festival)
ASSERT f.uuid IS UNIQUE;

MATCH (f:Festival),(d:Departement) WHERE  d.cd=f.cd
CREATE  (f)-[:FESTIVAL_IN_DEPARTEMENT]->(d);

LOAD CSV WITH HEADERS FROM
"file:///season-1819.csv" AS row FIELDTERMINATOR ','
MERGE( r:Rencontre{
    uuid:"",
    date: toString(row.Date),
    hometeam: toString(row.HomeTeam),
    opponent: toString(row.AwayTeam)
});

MATCH(r:Rencontre)
SET r.uuid=apoc.create.uuid();
CREATE CONSTRAINT ON (r:Rencontre) ASSERT r.uuid IS UNIQUE;

LOAD CSV WITH HEADERS FROM
"file:///stade.csv" AS row FIELDTERMINATOR ';'
MERGE( s:Stade{
    uuid:"",
    equipe: toString(row.equipe),
    lat: toFloat(row.lat),
    long: toFloat(row.long),
    ci: (CASE size(toString(row.ci)) WHEN 4 THEN "0"+toString(row.ci) ELSE toString(row.ci) END)
});

MATCH (s:Stade),(c:Commune) WHERE  s.ci=c.ci
CREATE  (s)-[:STADE_IN_COMMUNE]->(c);

MATCH(s:Stade)
SET s.uuid=apoc.create.uuid();
CREATE CONSTRAINT ON (s:Stade) ASSERT s.uuid IS UNIQUE;

MATCH(s:Stade), (r:Rencontre)
WHERE s.equipe=r.hometeam
create (r)-[:PLAY_AT_STADE]->(s);

LOAD CSV WITH HEADERS FROM 
"file:///fr-en-adresse-et-geolocalisation-etablissements-premier-et-second-degre.csv" AS row FIELDTERMINATOR ';'
WITH row WHERE NOT row.Latitude IS NULL
WITH row WHERE NOT row.Longitude IS NULL
MERGE( e:Etablissement{
    uuid:"",
    code: toString(row.Code_etablissement),
    lat: toFloat(row.Latitude),
    long:toFloat(row.Longitude),
    ci: (CASE size(toString(row.Code_commune)) WHEN 4 THEN "0"+toString(row.Code_commune) ELSE toString(row.Code_commune) END),
    type: "Premier/Second degrès"
});

LOAD CSV WITH HEADERS FROM
"file:///sup.csv" AS row FIELDTERMINATOR ';'
WITH row WHERE NOT row.com_code IS NULL
WITH row WHERE NOT row.coordonnees IS NULL
MERGE( e:Etablissement{
    uuid:"",
    code: toString(row.uai),
    lat: toFloat(split(row.coordonnees,",")[0]),
    long: toFloat(split(row.coordonnees,",")[1]),
    ci: (CASE size(toString(row.com_code)) WHEN 4 THEN "0"+toString(row.com_code) ELSE toString(row.com_code) END),
    type: "Supérieur"
});

MATCH(e:Etablissement)
SET e.uuid=apoc.create.uuid();
CREATE CONSTRAINT ON (e:Etablissement) ASSERT e.uuid IS UNIQUE;

MATCH (e:Etablissement),(c:Commune) WHERE  e.ci=c.ci
CREATE  (e)-[:SCHOOL_IN_COMMUNE]->(c);

LOAD CSV WITH HEADERS FROM 
"file:///muse.csv" AS row FIELDTERMINATOR ';'
WITH row WHERE NOT row.coordonnees_finales IS NULL
MERGE( m:Musee{
    uuid:"",
    nom_du_musee: toString(row.nom_du_musee),
    lat: toFloat(split(row.coordonnees_finales,",")[0]),
    long: toFloat(split(row.coordonnees_finales,",")[0])
});

MATCH(m:Musee)
SET m.uuid=apoc.create.uuid();
CREATE CONSTRAINT ON (m:Musee) ASSERT m.uuid IS UNIQUE;


LOAD CSV WITH HEADERS FROM 
"file:///liste-des-gares.csv" AS row FIELDTERMINATOR ';'
MERGE( g:Gare{
uuid: "",
code_uic: toString(row.code_uic),
libelle: toString(row.libelle),
nom_com: toUpper(toString(row.commune)),
nom_departement: toUpper(toString(row.departement)),
c_geo: toString(row.c_geo),
lat: toFloat(row.y_wgs84),
long: toFloat(row.x_wgs84)
});

MATCH(g:Gare)
SET g.uuid=apoc.create.uuid();
CREATE CONSTRAINT ON (g:Gare) ASSERT g.uuid IS UNIQUE;

MATCH (g:Gare),(d:Departement) WHERE  g.nom_departement=d.nom_departement
CREATE  (g)-[:GARE_IN_DEPARTEMENT]->(d);

LOAD CSV WITH HEADERS FROM 
"file:///indicateurs-loyers-maisons.csv" AS row FIELDTERMINATOR ';'
MERGE( loyer:Loyer{
    uuid: "",
    ci: (CASE size(toString(row.INSEE)) WHEN 4 THEN "0"+toString(row.INSEE) ELSE toString(row.INSEE) END),
    loypredm2: toFloat(replace(toString(row.loypredm2),',','.')),
    type: "Maison"
});


LOAD CSV WITH HEADERS FROM 
"file:///indicateurs-loyers-appartements.csv" AS row FIELDTERMINATOR ';'
MERGE( loyer:Loyer{
    uuid: "",
    ci: (CASE size(toString(row.INSEE)) WHEN 4 THEN "0"+toString(row.INSEE) ELSE toString(row.INSEE) END),
    loypredm2: toFloat(replace(toString(row.loypredm2),',','.')),
    type: "Appartement"
});

MATCH(loyer:Loyer)
SET loyer.uuid=apoc.create.uuid();

CREATE CONSTRAINT ON (loyer:Loyer)
ASSERT loyer.uuid IS UNIQUE;

MATCH (loyer:Loyer),(c:Commune) WHERE loyer.ci=c.ci 
CREATE  (loyer)-[:RENT_BY_COMMUNE]->(c);

LOAD CSV WITH HEADERS FROM 
"file:///hebergements_classes.csv" AS row FIELDTERMINATOR ';'
WITH row WHERE NOT row.CODE_POSTAL IS NULL
MERGE ( h:Hebergement{
    uuid: "",
    date_de_classement: row.DATE_DE_CLASSEMENT,
    typologie_etablisssement: row.TYPOLOGIE_ETABLISSEMENT,
    classement: row.CLASSEMENT,
    cp: toString(row.CODE_POSTAL),
    site_internet: row.SITE_INTERNET
});

MATCH(h:Hebergement)
SET h.uuid=apoc.create.uuid();
CREATE CONSTRAINT ON (h:Hebergement) ASSERT h.uuid IS UNIQUE;

LOAD CSV WITH HEADERS FROM 
"file:///etablissements-qt.csv" AS row FIELDTERMINATOR ','
WITH row WHERE NOT row.ADRESSE ="?"
WITH row WHERE NOT row.ADRESSE IS NULL
WITH row WHERE NOT row.CP IS NULL
WITH row WHERE NOT row.SITE_WEB_ETABLISSEMENT IS NULL
MERGE ( qt:Tourisme{
    uuid: "",
    adresse: row.ADRESSE,
    cp: (CASE size(toString(row.CP)) WHEN 4 THEN "0"+toString(row.CP) ELSE toString(row.CP) END),
    site_web_etablissement: row.SITE_WEB_ETABLISSEMENT,
    nom_com: toUpper(row.COMMUNE)
});

MATCH(qt:Tourisme)
SET qt.uuid=apoc.create.uuid();
CREATE CONSTRAINT ON (qt:Tourisme) ASSERT qt.uuid IS UNIQUE;

MATCH (c:Commune)-[:HAS_CP]->(cp:Cp),(qt:Tourisme)
WHERE c.nom_commune_postal=qt.nom_com
AND qt.cp=cp.cp
create (qt)-[:AT_COMMUNE]->(c);

MATCH (h:Hebergement), (qt:Tourisme)
WHERE qt.site_web_etablissement=h.site_internet
AND qt.cp=h.cp
AND qt.site_web_etablissement=h.site_internet
create (h)-[:HAS_A_LABEL]->(qt);