import os 
os.chdir("/home/aimane/Desktop/pyzo_")
import colnames

assiette_indices = {colnames.ID_BENEFICIAIRE_A : 0,
               colnames.ID_ADHERENT_A : 1,
               colnames.TYPE_ADHERENT : 3,
               colnames.TYPE_BENEFICIAIRE : 4,
               colnames.DNAISSANCE : 5,
               colnames.SEXE : 6,
               colnames.ASSIETTE_COTISATION : 8,
               colnames.DATE_FIN_DO : 9,
               colnames.VILLE : 10,
               colnames.NOMBRE_MOIS : 12} 

conso_indices = {colnames.ID_BENEFICIAIRE_C : 1,
            colnames.ID_ADHERENT_C : 0,
            colnames.RANG : 2,
            colnames.DATE_DEBUT_SOIN : 5, 
            colnames.CODE_ACTE : 9,
            colnames.LIBELLE_ACTE : 10,
            colnames.PRIX_UNITAIRE : 13}

ald_indices = {colnames.ID_ADHERENT_ALD : 0,
          colnames.ID_BENEFICIAIRE_ALD : 1,
          colnames.RANG_ALD : 2,
          colnames.CODE_ALD_ALC : 3,
          colnames.DATE_DEBUT_ACCORD_ALD : 4,
          colnames.DATE_FIN_ACCORD_ALD : 5}


