#' Get constellate papers based on search criteria
#'
#' @import dplyr
#' @import arrow
#' @import duckdb
#'
#' @param cnstl_dir File path for directory with constellate data (Arrow will
#' grab all files within this folder and subfolders).
#' @param fields Vector of fields to return. Using * to return all fields
#' may result in errors.
#' @param target_jounals Vector of target journal names. If NULL, papers
#' from all journals will be returned.
#' @param text_search Vector of strings to match in texts. Texts will only
#' be returned that contain at least one of the strings in this vector. If NULL,
#' all papers will be returned. Search is not case sensitive.
#' @param use_stable_names If TRUE, a stable_name field will be returned that
#' lists the most recent name of journals that have changed names over time. If
#' target_journals has been specified, all journals that share the target
#' journals' stable names will be added to your search.
#' @param min_year If provided, results will exclude papers from before min_year
#' @param max_year If provided, results will exclude papers from after max_year
#' @param limit If provided, will return only this number of papers (for testing)
#' @param return_query If TRUE, return the query instead of results. To be used
#' when you want to add to the base query
#' @returns A dataframe with results of query
#' @export
#'
get_cnstl <- function(cnstl_dir, fields, target_journals=NULL,
                      text_search=NULL, use_stable_names=FALSE,
                      min_year=NULL, max_year=NULL, limit=NULL,
                      return_query=FALSE) {

  # Set up Arrow access to data
  con_dat <- open_dataset(cnstl_dir, # Point to directory
                          format = "csv")

  # Create a (local) database
  con <- DBI::dbConnect(duckdb::duckdb())
  # Add our data (from Arrow) as a table
  cnstl_tbl <- to_duckdb(con_dat, con, "cnstl")

  # If stable_names are requested, load the crosswalk
  if (use_stable_names) {
    stable_names <- read.csv('data/journal_stable_names.csv')
  }

  # Turn vector of desired fields into query string
  fields_str <- paste(fields, collapse=', ')

  # Build base of query
  query <- paste0("SELECT ", fields_str, " FROM cnstl")

  # Set string so that only first condition gets "WHERE"
  and_where <- ' WHERE '

  # Limit to target journals, if specified
  if (!is.null(target_journals)) {
    # If using stable names, expand list to include other names of journal
    if (use_stable_names) {
      target_stable_names <- stable_names |>
        filter(isPartOf %in% target_journals) |> pull(stable_name)
      target_journals <- stable_names$isPartOf[stable_names$stable_name %in% target_stable_names]
    }

    # Turn vector of journals into query string
    journal_str <- paste0("'", target_journals, "'", collapse=', ')

    # Add journal requirement to query
    query <- paste0(query, ' WHERE (isPartOf IN (', journal_str, '))')

    # Set and/where string for additional conditions
    and_where <- ' AND '
  }

  # Limit to papers containing target substrings, if specified
  if (!is.null(text_search)) {
    # Convert list of search terms to query string
    text_query <- paste0("LOWER(fullText) LIKE '%", text_search,
                         "%'", collapse=' OR ')

    # Add to query
    query <- paste0(query, and_where, '(', text_query, ')')

    # Set and/where string for additional conditions
    and_where <- ' AND '
  }

  # Limit to year range, if specified
  if (!is.null(min_year)) {
    query <- paste0(query, and_where, '(publicationYear >= ', min_year, ')')

    # Set and/where string for additional conditions
    and_where <- ' AND '
  }

  if (!is.null(min_year)) {
    query <- paste0(query, and_where, '(publicationYear <= ', max_year, ')')

    # Set and/where string for additional conditions
    and_where <- ' AND '
  }

  # Limit max number of results
  if (!is.null(limit)) {
    query <- paste0(query, ' LIMIT ', limit)
  }

  print(query)
  if (return_query) {
    return(query)
  }

  result <- as.data.frame(DBI::dbGetQuery(con, query))

  # if using stable names AND journal names are returned, add stable names to output
  if (('isPartOf' %in% colnames(result)) & use_stable_names) {
    result <- merge(result, stable_names, by='isPartOf', all.x=TRUE)
  }

  DBI::dbDisconnect(con)
  return(result)

}

#' List all fields in Constellate Data
#'
#' @import dplyr
#' @import arrow
#' @import duckdb
#'
#' @param cnstl_dir File path for directory with constellate data (Arrow will
#' grab all files within this folder and subfolders)
#' @export
#' @returns Character vector containing the field names in the Constellate data
list_cnstl_fields <- function(cnstl_dir) {
  con_dat <- open_dataset(cnstl_dir, # Point to directory
                          format = "csv")

  # Create a (local) database
  con <- DBI::dbConnect(duckdb::duckdb())
  # Add our data (from Arrow) as a table
  cnstl_tbl <- to_duckdb(con_dat, con, "cnstl")
  fields <- DBI::dbGetQuery(con, 'SELECT * FROM cnstl LIMIT 0') |> as.data.frame()

  DBI::dbDisconnect(con)

  return(fields)
}

#' Get journal-level (or journal-by-year) counts of how many papers
#' contain matching text
#'
#' @import arrow
#' @import duckdb
#' @import dplyr
#'
#' @param cnstl_dir File path for directory with constellate data (Arrow will
#' grab all files within this folder and subfolders)
#' @param text_search Vector of strings to match in texts. Texts will only
#' be returned that contain at least one of the strings in this vector.
#' @param by_year If TRUE, results will be aggregated at the
#' journal-by-year level
#'
#' @export
#' @returns A dataframe with counts of matching papers in each journal
#' (or journal-by-year if by_year is set to TRUE)
journal_count <- function(cnstl_dir, text_search, by_year=FALSE) {
  # Build aggregating query
  text_q <- paste0("LOWER(fullText) LIKE '%", text_search,
                   "%'", collapse=' OR ')
  text_q <- paste0('(', text_q, ')')
  query <- paste0("SELECT isPartOf, ", "publicationYear, "[by_year],
                  "COUNT(CASE WHEN ",
                  text_q, " THEN 1 ELSE NULL END)",
                  " AS matched_papers, COUNT(*) AS total_papers FROM cnstl GROUP BY isPartOf")

  # Set to journal-by-year level if specified
  if (by_year) {
    query <- paste0(query, ', publicationYear')
  }

  con_dat <- open_dataset(cnstl_dir, # Point to directory
                          format = "csv")

  # Create a (local) database
  con <- DBI::dbConnect(duckdb::duckdb())
  # Add our data (from Arrow) as a table
  cnstl_tbl <- to_duckdb(con_dat, con, "cnstl")
  journal_level <- DBI::dbGetQuery(con, query) |>
    arrange(desc(isPartOf))

  DBI::dbDisconnect(con)

  return(journal_level)
}

#' Get annual level counts of how many papers match a term or set of terms
#'
#' @import arrow
#' @import duckdb
#' @import dplyr
#'
#' @param cnstl_dir File path for directory with constellate data (Arrow will
#' grab all files within this folder and subfolders)
#' @param text_search Vector of strings to match in texts.
#'
#' @export
#' @returns A dataframe with annual counts of how many times the specified terms
#' appear
text_match_time_series <- function(cnstl_dir, text_search) {
  text_q <- paste0("LOWER(fullText) LIKE '%", text_search,
                   "%'", collapse=' OR ')
  text_q <- paste0('(', text_q, ')')
  query <- paste0("SELECT publicationYear, ",
                  "COUNT(CASE WHEN ",
                  text_q, " THEN 1 ELSE NULL END)",
                  " AS matched_papers, COUNT(*) AS total_papers FROM cnstl GROUP BY publicationYear")


  con_dat <- open_dataset(cnstl_dir, # Point to directory
                          format = "csv")
  # Create a (local) database
  con <- DBI::dbConnect(duckdb::duckdb())
  # Add our data (from Arrow) as a table
  cnstl_tbl <- to_duckdb(con_dat, con, "cnstl")
  annual_level <- DBI::dbGetQuery(con, query) |>
    arrange(desc(publicationYear))

  DBI::dbDisconnect(con)

  return(annual_level)
}

#' Plot term series of how frequently term is used in corpus
#'
#' @import ggplot2
#'
#' @param cnstl_dir File path for directory with constellate data (Arrow will
#' grab all files within this folder and subfolders)
#' @param text_search Vector of strings to match in texts.
#'
#' @export
#' @returns A ggplot object.
plot_time_series_by_term <- function(cnstl_dir, text_search) {
  df <- NULL
  for (term in text_search) {
    tdf <- text_match_time_series(cnstl_dir, term)
    tdf$term <- term
    if (is.null(df)) {
      df <- tdf
    }
    else df <- rbind(df, tdf)
  }

  plt <- ggplot(df, aes(x=publicationYear, y=matched_papers, color=term)) +
    geom_line() + xlab('Publication Year') + ylab('Matched Papers') +
    ggtitle('Number of Matched Papers by Year')
  return(plt)

}
