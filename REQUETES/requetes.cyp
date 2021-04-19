// REQUETE 1 : Les communes avec le plus d'accidents
MATCH (a:Accident)-[:TOOK_PLACE]->(l:Lieu)-[:ACCIDENT_IN_COMMUNE]->(c:Commune) 
RETURN c.nom_commune_postal,count(a) AS Nombres_Accident 
ORDER BY Nombres_Accident DESC LIMIT 5

//1bis: le jour avec le plus d'accidents

MATCH (d:Date) 
RETURN apoc.date.format(apoc.date.parse((CASE size(toString(d.jour)) WHEN 1 THEN "0"+toString(d.jour) ELSE toString(d.jour) END)+"/"+
(CASE size(toString(d.mois)) WHEN 1 THEN "0"+toString(d.mois) ELSE toString(d.mois) END)
+"/"+toString(d.an),'ms','dd/MM/yyyy'),'ms','dd/MM/yyyy') as Date, count(apoc.date.format(apoc.date.parse((CASE size(toString(d.jour)) 
WHEN 1 THEN "0"+toString(d.jour) ELSE toString(d.jour) END)+"/"+(CASE size(toString(d.mois)) WHEN 1 THEN "0"+toString(d.mois) ELSE toString(d.mois) END)
+"/"+toString(d.an),'ms','dd/MM/yyyy'),'ms','dd/MM/yyyy')) as NbAccidents order by count(apoc.date.format(apoc.date.parse((CASE size(toString(d.jour)) WHEN 1 THEN "0"
+toString(d.jour) ELSE toString(d.jour) END)+"/"+(CASE size(toString(d.mois)) WHEN 1 THEN "0"+toString(d.mois) ELSE toString(d.mois) END)+"/"
+toString(d.an),'ms','dd/MM/yyyy'),'ms','dd/MM/yyyy')) DESC LIMIT 5; 

//2bis: Le ration d'accidents par nombre d'habitants

MATCH (a:Accident)-[:TOOK_PLACE]->(l:Lieu)-[:ACCIDENT_IN_COMMUNE]->(c:Commune)-[:COMMUNE_POPULATION]->(p:Population) 
RETURN c.nom_commune_postal, (count(a)/p.population_totale) AS Ratio 
ORDER BY Ratio DESC LIMIT 5

//** REQUETE 3 : 
// Parmi les véhicules hybrides-électrique et électriques concernés par un accident pendant l’année 2019, où se trouve la borne la plus proche du lieu de l’accident ?
MATCH (l:Lieu)<-[:TOOK_PLACE]-(a:Accident)-[:AT_DATE]->(d:Date), (a:Accident)-[:VEHICULE_CONCERNED]->(v:Vehicule) , (b:Borne)
WHERE v.motor IN ["2","3"]
RETURN a.num_acc, v.motor, apoc.date.format(apoc.date.parse((CASE size(toString(d.jour)) WHEN 1 THEN "0"+toString(d.jour) ELSE toString(d.jour) END)+"/"+
(CASE size(toString(d.mois)) WHEN 1 THEN "0"+toString(d.mois) ELSE toString(d.mois) END)
+"/"+toString(d.an),'ms','dd/MM/yyyy'),'ms','dd/MM/yyyy') AS Date_Accident, round(distance(point({ latitude: l.lat, longitude: l.long  }), point({ latitude: b.lat,  longitude: b.long }))) AS travelDistance
ORDER BY travelDistance ASC
LIMIT 1

// REQUEST 4
//Quelle est la moyenne d'âge des personnes impliqués dans un accident dans un rayon d’X kilomètres autour des établissements d’enseignement ?

MATCH (l:Lieu)<-[:TOOK_PLACE]-(a:Accident)-[:AT_DATE]->(d:Date),(a:Accident)-[:USAGER_CONCERNED]->(u:Usager), (e:Etablissement)
WHERE round(distance(point({ latitude: e.lat, longitude: e.long  }), point({ latitude: l.lat,  longitude: l.long })))<10000
return e.code, e.type, round(avg(date().year-u.an_nais))

// REQUEST 6
//Quels sont les accidents impliqués aux alentours d’un stade lors d’un match de football de ligue 1 ou d’un festival ? 

//Festival
MATCH (f:Festival), (d:Date)<-[:AT_DATE]-(a:Accident)-[:TOOK_PLACE]->(l:Lieu)
WHERE date(f.date_debut_ancien)
<=date(d.an+"-"+(CASE size(d.mois) WHEN 1 THEN "0"+d.mois ELSE d.mois END)+"-"+(CASE size(d.jour) WHEN 1 THEN "0"+d.jour ELSE d.jour END))
AND date(f.date_de_fin_ancien)
>= date(d.an+"-"+(CASE size(d.mois) WHEN 1 THEN "0"+d.mois ELSE d.mois END)+"-"+(CASE size(d.jour) WHEN 1 THEN "0"+d.jour ELSE d.jour END))
AND round(distance(point({ latitude: l.lat, longitude: l.long  }), point({ latitude: f.lat,  longitude: f.long })))<10000
RETURN a.num_acc,f.nom_de_la_manifestation, f.domaine ,date(d.an+"-"+(CASE size(d.mois) WHEN 1 THEN "0"+d.mois ELSE d.mois END)+"-"+(CASE size(d.jour) WHEN 1 THEN "0"+d.jour ELSE d.jour END)) AS Date_Evenement
, round(distance(point({ latitude: l.lat, longitude: l.long  }), point({ latitude: f.lat,  longitude: f.long }))) 
AS travelDistance ORDER BY travelDistance ASC LIMIT 5

//FootBall
MATCH (d:Date)<-[:AT_DATE]-(a:Accident)-[:TOOK_PLACE]->(l:Lieu), (r:Rencontre)-[:PLAY_AT_STADE]->(s:Stade)
WHERE date(split(r.date,'/')[2]+"-"+split(r.date,'/')[1]+"-"+split(r.date,'/')[0])=date(d.an+"-"+(CASE size(d.mois) WHEN 1 THEN "0"+d.mois ELSE d.mois END)+"-"+(CASE size(d.jour) WHEN 1 THEN "0"+d.jour ELSE d.jour END))
AND round(distance(point({ latitude: l.lat, longitude: l.long  }), point({ latitude: s.lat,  longitude: s.long })))<10000
RETURN a.num_acc ,date(d.an+"-"+(CASE size(d.mois) WHEN 1 THEN "0"+d.mois ELSE d.mois END)+"-"+(CASE size(d.jour) WHEN 1 THEN "0"+d.jour ELSE d.jour END)) as Date_Rencontre
, r.hometeam+" VS "+r.opponent AS Rencontre,round(distance(point({ latitude: l.lat, longitude: l.long  }), point({ latitude: s.lat,  longitude: s.long }))) 
AS travelDistance ORDER BY travelDistance ASC LIMIT 5

//CLUB LE PLUS ACCIDENTOGENE
MATCH (d:Date)<-[:AT_DATE]-(a:Accident)-[:TOOK_PLACE]->(l:Lieu), (r:Rencontre)-[:PLAY_AT_STADE]->(s:Stade)
WHERE date(split(r.date,'/')[2]+"-"+split(r.date,'/')[1]+"-"+split(r.date,'/')[0])=date(d.an+"-"+(CASE size(d.mois) WHEN 1 THEN "0"+d.mois ELSE d.mois END)+"-"+(CASE size(d.jour) WHEN 1 THEN "0"+d.jour ELSE d.jour END))
AND round(distance(point({ latitude: l.lat, longitude: l.long  }), point({ latitude: s.lat,  longitude: s.long })))<10000
RETURN r.hometeam, count(a) as Nombre_Accidents ORDER BY Nombre_Accidents DESC LIMIT 1

//REQUEST 7
MATCH (qt:Tourisme), (m:Musee), (f:Festival), (g:Gare)
CALL apoc.spatial.geocodeOnce(qt.adresse+" "+qt.cp+" "+"France")
YIELD location
WHERE round(distance(point({ latitude: location.latitude, longitude: location.longitude  }), point({ latitude: m.lat,  longitude: m.long })))<100000 
AND round(distance(point({ latitude: location.latitude, longitude: location.longitude  }), point({ latitude: f.lat,  longitude: f.long })))<100000
RETURN qt.adresse limit 1;

// REQUEST 8
// Changer has a train station en gare in dep, has a bornes
MATCH (l:Loyer)-[:RENT_BY_COMMUNE]->(c:Commune)-[:AT_DEPARTEMENT]->(d:Departement)<-[:GARE_IN_DEPARTEMENT]-(g:Gare)
return c.nom_commune_postal, count(distinct g), (sum(distinct l.loypredm2)/2) as Moyenne_Loyer_Metre_Carre order by Moyenne_Loyer_Metre_Carre DESC

MATCH (g:Gare)-[:GARE_IN_DEPARTEMENT]->(d:Departement)<-[:AT_DEPARTEMENT]-(c:Commune)<-[:RENT_BY_COMMUNE]-(loyer:Loyer), (b:Borne)-[:BORNE_IN_COMMUNE]->(c:Commune)<-[:ETABLISSEMENTS_IN_COMMUNE]-(ev:EtablissementV)
return c.nom_commune_postal, c.ci, (sum(loyer.loypredm2)/2), count(distinct g.uuid) , count(distinct b.uuid), count(distinct ev.uuid) 
order by count(distinct g.uuid) asc
UNION
MATCH (g:Gare)-[:GARE_IN_DEPARTEMENT]->(d:Departement)<-[:AT_DEPARTEMENT]-(c:Commune)<-[:RENT_BY_COMMUNE]-(loyer:Loyer), (b:Borne)-[:BORNE_IN_COMMUNE]->(c:Commune)<-[:ETABLISSEMENTS_IN_COMMUNE]-(ev:EtablissementV)
return c.nom_commune_postal, c.ci, (sum(loyer.loypredm2)/2), count(distinct g.uuid), count(distinct b.uuid), count(distinct ev.uuid) 
order by count(distinct b.uuid), count(distinct ev.uuid)  desc

//REQUEST 9
MATCH (h:Hebergement)-[:HAS_A_LABEL]->(qt:Tourisme)-[:AT_COMMUNE]->(c:Commune)-[:AT_DEPARTEMENT]->(d:Departement{cd:"83"})<-[:FESTIVAL_IN_DEPARTEMENT]-(f:Festival{nom_de_la_manifestation:"JAZZ A RAMATUELLE"})
CALL apoc.spatial.geocodeOnce(qt.adresse+" "+qt.cp+" "+"France")
YIELD location
WHERE split(h.classement," ")[1]="étoiles"
AND toInteger(split(h.classement," ")[0])>4
RETURN h.typologie_etablisssement ,h.classement ,
min(round(distance(point({ latitude: toFloat(location.latitude) , longitude: toFloat(location.longitude)}), 
point({ latitude: f.lat,  longitude: f.long })))) AS travelDistance

//la cathegorie de véhicules les plus accidentées et la moyenne d'age de leurs conducteurs
MATCH (u:Usager{grav:"2"})<-[:USAGER_CONCERNED]-(a:Accident)-[:VEHICULE_CONCERNED]->(v:Vehicule)
RETURN DISTINCT(v.catv), COUNT(u) AS nbMorts, round(avg(date().year-u.an_nais)) AS ageMoyen ORDER BY nbMorts DESC

MATCH (u:Usager{grav:'2'})<-[:USAGER_CONCERNED]-(a:Accident)-[:CARACTERISTIQUES]->(ca:CAccident),
(a:Accident)-[:TOOK_PLACE]->(l:Lieu)-[:CARACTERISTIQUES_LIEU]->(cl:CLieu),
(l:Lieu)-[:ACCIDENT_IN_COMMUNE]->(c:Commune)-[:AT_DEPARTEMENT]->(d:Departement)-[:AT_REGION]->(r:Region{Nom_Region:"Île-de-France"})
WHERE (ca.atm IN ["2","3"]) AND (ca.lum IN ["3","4","5"]) 
RETURN COUNT(u)