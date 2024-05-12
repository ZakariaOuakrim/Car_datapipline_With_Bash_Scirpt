#!/bin/bash
CLEAN_PAIRS=()
# Fonction d'affichage de l'aide
print_usage() {
    echo "Usage: $(basename "$0") [OPTIONS] db_name table_name output_csv [limit]"
    echo "Example: $(basename "$0") -e mydb mytable data.csv 100"
    echo ""
    echo "Options:"
    echo "  -e : Export data from table to CSV file (default)"
    echo "  -i : Import data from CSV file to table"
    echo "  -h : Display help"
    echo "  -c table.column : Clean NULL values from specified table and column"
    echo "  -v db_name table_name : Verify NULL values in specified table"
    echo "  -d db_name : Create a new database"
    exit 1
}

# Capture all the usage of code
capture_usage() {
    local usage_file="./usage.log"
    echo "$(date): $(basename "$0") $@" >>"$usage_file"
}

# Fonction d'affichage des erreurs MySQL
print_mysql_error() {
    echo "Erreur MySQL: $1" >&2
    exit 1
}

# Fonction d'exportation des données vers un fichier CSV
export_data() {
    local db_name="$2"
    local table_name="$3"
    local output_csv="$4"
    local limit="$5"
    # Requête SQL SELECT
    local select_query="SELECT * FROM $table_name"
    if [ -n "$limit" ]; then
        select_query="$select_query LIMIT $limit"
    fi
    select_query="$select_query;"
    # Création du fichier CSV avec le nom spécifié
    touch "$output_csv" || {
        echo "Impossible de créer le fichier $output_csv" >&2
        exit 1
    }
    # Exécution de la requête via MySQL et exportation des résultats vers le fichier CSV
    mysql -u houssam -p -p -e "$select_query" "$db_name" | sed 's/\t/","/g;s/^/"/;s/$/"/;s/\n//g' >"$output_csv"

    # Vérification du statut de la commande
    if [ $? -eq 0 ]; then
        echo "Données exportées vers $output_csv avec succès."
    else
        echo "Erreur lors de l'exportation des données vers $output_csv."
    fi
}

# Fonction d'importation des données depuis un fichier CSV
import_data() {
    local db_name="$2"
    local table_name="$3"
    local input_csv="$4"
    # Vérification de l'existence du fichier CSV
    if [ ! -f "$input_csv" ]; then
        echo "Le fichier CSV '$input_csv' n'existe pas."
        exit 1
    fi
    # Lecture du fichier CSV et insertion des données dans la table
    echo "Importation des données depuis '$input_csv' dans la table '$table_name'..."
    # Lecture de la première ligne du fichier CSV pour obtenir les noms de colonnes
    IFS=',' read -r -a columns <"$input_csv"
    # Création de la chaîne de colonnes pour la requête INSERT
    local column_names=$(printf "%s," "${columns[@]}")
    column_names=${column_names%,} # Suppression de la dernière virgule
    # Boucle pour lire le fichier CSV ligne par ligne et insérer les données dans la table
    tail -n +2 "$input_csv" | while IFS=',' read -r -a row; do
        # Formater les valeurs pour les insérer dans la requête SQL
        local values=$(printf "'%s'," "${row[@]}")
        values=${values%,} # Suppression de la dernière virgule

        # Insertion des données dans la table MySQL
        mysql -u houssam -p "$db_name" -e "INSERT INTO $table_name ($column_names) VALUES ($values);" || {
            echo "Erreur lors de l'insertion des données." >&2
            exit 1
        }
    done
    echo "Données importées avec succès."
}
# Fonction to clean null value
clean_null_values() {
    # Check if table and column pairs are provided for cleaning
    if [ ${#CLEAN_PAIRS[@]} -eq 0 ]; then
        echo "Table and column pairs to clean are required. Use the -c option." >&2
        print_usage
    fi
    echo "Pairs to clean: ${CLEAN_PAIRS[@]}"

    # Iterate through each table and column pair and clean NULL values
    for pair in "${CLEAN_PAIRS[@]}"; do
        # Extract database, table, and column using regular expression
        if [[ "$pair" =~ ([^.]+)\.([^.]+)\.([^[:space:]]+) ]]; then
            DB="${BASH_REMATCH[1]}"
            TABLE="${BASH_REMATCH[2]}"
            COLUMN="${BASH_REMATCH[3]}"
        else
            echo "Invalid pair format: $pair" >&2
            continue
        fi

        # Clean NULL values
        echo "Cleaning NULL values from $TABLE.$COLUMN..."
        clean_query="DELETE FROM $TABLE WHERE $COLUMN IS NULL;"
        mysql -h localhost -u houssam -p -p"$MYSQL_PASSWORD" "$DB" -e "$clean_query"
        if [ $? -eq 0 ]; then
            echo "NULL values cleaned successfully from $TABLE.$COLUMN."
            exit 1
        else
            echo "Error cleaning NULL values from $TABLE.$COLUMN."
            exit 1
        fi
    done
}

# Fonction pour vérifier la présence de valeurs NULL dans une colonne spécifique
verify_null_values() {
    local db_name="$2"
    local table_name="$3"

    # Vérification des paramètres
    if [ -z "$db_name" ] || [ -z "$table_name" ]; then
        echo "Les paramètres de la base de données et de la table sont requis pour vérifier les valeurs NULL." >&2
        exit 1
    fi

    # Récupération de la liste des colonnes de la table
    columns=$(mysql -u houssam -p -N -B -e "SHOW COLUMNS FROM $table_name" "$db_name" | awk '{print $1}')

    # Boucle pour vérifier chaque colonne en utilisant fork pour chaque colonne
    for column in $columns; do
        (
            # Vérification de la présence de valeurs NULL dans la colonne
            null_count=$(mysql -u houssam -p -N -B -e "SELECT COUNT(*) FROM $table_name WHERE $column IS NULL" "$db_name")
            if [ "$null_count" -gt 0 ]; then
                echo "La colonne $column de la table $table_name contient des valeurs NULL."
            else
                echo "La colonne $column de la table $table_name ne contient pas de valeurs NULL."
            fi
        ) &
    done

    # Attente de la fin de tous les processus fils
    wait
}

# Function to create a database if it doesn't exist
create_database() {
    local db_name="$1"
    echo "Creating database '$db_name'..."

    # Check if the database already exists
    existing_db=$(mysql -u houssam -p -e "SHOW DATABASES LIKE '$db_name'" | grep "$db_name")
    if [ -n "$existing_db" ]; then
        echo "Database '$db_name' already exists."
        exit 1
    fi

    # Create the database
    mysql -u houssam -p -e "CREATE DATABASE $db_name"
    if [ $? -eq 0 ]; then
        echo "Database '$db_name' created successfully."
    else
        echo "Error creating database '$db_name'."
        exit 1
    fi
}

# Function to drop a database with admin privileges
drop_database() {
    local db_name="$1"
    echo "Dropping database '$db_name'..."
    # Check if the database exists
    existing_db=$(mysql -u houssam -p -e "SHOW DATABASES LIKE '$db_name'" | grep "$db_name")
    if [ -z "$existing_db" ]; then
        echo "Database '$db_name' does not exist."
        exit 1
    fi
    # Drop the database
    mysql -u houssam -p -e "DROP DATABASE $db_name"
    if [ $? -eq 0 ]; then
        echo "Database '$db_name' dropped successfully."
    else
        echo "Error dropping database '$db_name'."
        exit 1
    fi
}

# Function to create a clone of a database
create_database_clone() {
    local source_db="$2"
    local clone_db="$3"
    echo "Creating clone of database '$source_db' as '$clone_db'..."
    # Check if the source database exists
    existing_db=$(mysql -u houssam -p -e "SHOW DATABASES LIKE '$source_db'" | grep "$source_db")
    if [ -z "$existing_db" ]; then
        echo "Source database '$source_db' does not exist."
        exit 1
    fi
    # Check if the clone database already exists
    existing_clone_db=$(mysql -u houssam -p -e "SHOW DATABASES LIKE '$clone_db'" | grep "$clone_db")
    if [ -n "$existing_clone_db" ]; then
        echo "Clone database '$clone_db' already exists."
        exit 1
    fi
    # Create the clone database by copying the structure and data from the source database
    mysqldump -u houssam -p "$source_db" >dump.sql
    mysql -u houssam -p -e "CREATE DATABASE $clone_db"
    mysql -u houssam -p "$clone_db" <dump.sql
    rm dump.sql
    if [ $? -eq 0 ]; then
        echo "Clone database '$clone_db' created successfully."
    else
        echo "Error creating clone database '$clone_db'."
        exit 1
    fi
}

# Function to display the usage log
display_usage_log() {
    local usage_file="./usage.log"
    echo "Usage log:"
    cat "$usage_file"
}

#-----------------------------------Merge---------------------------------------------

merge_table() {
    local db_name1="$2"
    local table_name1="$3"
    local db_name2="$4"
    local table_name2="$5"
    local output_csv="$6"

    local select_query1="SELECT * FROM $table_name1"
    local select_query2="SELECT * FROM $table_name2"

    touch "$output_csv" || {
        echo "Impossible de créer le fichier $output_csv" >&2
        exit 1
    }

    local query_output1=$(mysql -u root -p -h localhost $db_name1 -e "$select_query1" 2>&1)
    local query_output2=$(mysql -u root -p -h localhost $db_name2 -e "$select_query2" 2>&1)

    local timestamp=$(date "+%Y-%m-%d-%H-%M-%S")
    local username=$(whoami)

    if [[ $query_output1 == *"ERROR"* ]]; then
        echo "$timestamp : $username : ERROR : An error occurred while fetching data from database $db_name1." >>history.log
        exit 1
    fi

    if [[ $query_output2 == *"ERROR"* ]]; then
        echo "$timestamp : $username : ERROR : An error occurred while fetching data from database $db_name2." >>history.log
        exit 1
    fi

    paste -d, <(echo "$query_output1") <(echo "$query_output2") >"$output_csv"

    if [ $? -eq 0 ]; then
        echo "$timestamp : $username : INFOS : Tables merged successfully. Merged data saved in $output_csv" >>history.log
        echo "Tables merged successfully. Merged data saved in $output_csv"
    else
        echo "$timestamp : $username : ERROR : An error occurred while merging tables." >>history.log
        exit 1
    fi
}

merge_table_based_on_id() {
    {
        local db_name1="$2"
        local table_name1="$3"
        local db_name2="$4"
        local table_name2="$5"
        local join_column="$6"
        local output_csv="$7"

        local select_query="SELECT $table_name1.*, $table_name2.* FROM $table_name1 JOIN $db_name2.$table_name2 ON $table_name1.$join_column = $table_name2.$join_column"

        touch "$output_csv" || {
            echo "Impossible de créer le fichier $output_csv" >&2
            exit 1
        }

        mysql -u root -p -h localhost $db_name1 -e "$select_query" >"$output_csv"

        #date flinux
        local timestamp=$(date "+%Y-%m-%d-%H-%M-%S")
        local username=$(whoami)

        #kanchekiw wach dakchi dkhl mzyan csv ola la
        if [ -s "$output_csv" ]; then
            echo "Merge completed successfuly"
            echo "$timestamp : $username : INFOS : Tables merged successfully. Merged data saved in $output_csv" >>history.log
        else
            echo "An error occurred Merge uncompleted"
            echo "$timestamp : $username : ERROR : An error occurred while merging tables." >>history.log
            exit 1
        fi
    } || {
        #hna kan handliw error
        local timestamp=$(date "+%Y-%m-%d-%H-%M-%S")
        local username=$(whoami)
        echo "$timestamp : $username : ERROR : An error occurred while merging tables." >>history.log
        exit 1
    }
}

merge_table_clean_threads() {
    local db_name1="$3"
    local table_name1="$4"
    local column_name1="$5"
    local db_name2="$6"
    local table_name2="$7"
    local column_name2="$8"
    local output_csv="$9"

    local select_query1="SELECT * FROM $table_name1"
    local select_query2="SELECT * FROM $table_name2"

    touch "$output_csv" || {
        echo "Impossible de créer le fichier $output_csv" >&2
        exit 1
    }

    clean_table1() {
        mysql -u root -p -h localhost $db_name1 -e "DELETE FROM $table_name1 WHERE $column_name1 IS NULL" || {
            echo "Erreur lors du nettoyage de la table $table_name1." >&2
            exit 1
        }
    }

    clean_table2() {
        mysql -u root -p -h localhost $db_name2 -e "DELETE FROM $table_name2 WHERE $column_name2 IS NULL" || {
            echo "Erreur lors du nettoyage de la table $table_name2." >&2
            exit 1
        }
    }

    clean_table1 &

    clean_table2 &

    wait

    local query_output1=$(mysql -u root -p -h localhost $db_name1 -e "$select_query1" 2>&1)
    local query_output2=$(mysql -u root -p -h localhost $db_name2 -e "$select_query2" 2>&1)

    local timestamp=$(date "+%Y-%m-%d-%H-%M-%S")
    local username=$(whoami)

    if [[ $query_output1 == *"ERROR"* ]]; then
        echo "$timestamp : $username : ERROR : An error occurred while fetching data from database $db_name1." >>history.log
        exit 1
    fi

    if [[ $query_output2 == *"ERROR"* ]]; then
        echo "$timestamp : $username : ERROR : An error occurred while fetching data from database $db_name2." >>history.log
        exit 1
    fi

    paste -d, <(echo "$query_output1") <(echo "$query_output2") >"$output_csv"

    if [ $? -eq 0 ]; then
        echo "$timestamp : $username : INFOS : Tables merged successfully. Merged data saved in $output_csv" >>history.log
        echo "Tables merged successfully. Merged data saved in $output_csv"
    else
        echo "$timestamp : $username : ERROR : An error occurred while merging tables." >>history.log
        exit 1
    fi
}

while getopts ":ehicvdrnl:" opt; do
    case ${opt} in
    e)
        capture_usage "-e"
        export_data "$@"
        exit 0
        ;;
    i)
        capture_usage "-i"
        import_data "$@"
        exit 0
        ;;
    h)
        capture_usage "-h"
        print_usage
        ;;
    c)
        capture_usage "-c $OPTARG"
        CLEAN_PAIRS+=("$OPTARG")
        clean_null_values
        ;;
    v)
        capture_usage "-v $@"
        verify_null_values "$@"
        exit 0
        ;;
    d)
        capture_usage "-d $OPTARG"
        create_database "$OPTARG"
        exit 0
        ;;
    r)
        capture_usage "-r $OPTARG"
        drop_database "$OPTARG"
        exit 0
        ;;
    n)
        capture_usage "-n $@"
        create_database_clone "$@"
        exit 0
        ;;
    l)
        capture_usage "-l"
        display_usage_log
        exit 0
        ;;
    #----------------------------------Merge-----------------------------
    m)
        if [ "$2" == "--help" ]; then
            echo "Usage: ./file -m <db_name1> <table_name1> <db_name2> <table_name2> <Union_id> <output_csv>"
            echo "Merges two tables from different databases."
            echo "Arguments:"
            echo "  db_name1: Name of the first database"
            echo "  table_name1: Name of the first table"
            echo "  db_name2: Name of the second database"
            echo "  table_name2: Name of the second table"
            echo "  Union_id: Optional if you want to merge by id"
            echo "  output_csv: Output CSV file"
            exit 0
        #clean with threads part
        elif [ "$2" == "-ct" ]; then
            echo "clean threads"
            if [ $# -eq 9 ]; then
                merge_table_clean_threads "$@"
            else
                echo "Invalid number of arguments."
            fi
            exit 0
        fi

        if [ $# -eq 6 ]; then
            merge_table "$@"
        elif [ $# -eq 7 ]; then
            merge_table_based_on_id "$@"
        else
            echo "Invalid number of arguments. "
            exit 1
        fi
        exit 0
        ;;
    \?)
        echo "Option invalide: -$OPTARG" >&2
        print_usage
        ;;
    esac
done

# Shift option arguments
shift $((OPTIND - 1))

# Vérification du nombre d'arguments
if [ "$#" -lt 3 ] || [ "$#" -gt 4 ]; then
      print_usage
fi






