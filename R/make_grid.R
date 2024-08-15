#' # x = list(
#' #   a = 1:3,
#' #   b = list(
#' #     b1 =4:5,
#' #     b2 = 5:7,
#' #     b3 = list(b31 =10:11,
#' #          b32 = 12:13),
#' #     b4 = list(b41=14:15,
#' #          b42 = 16:17)
#' #   )
#' # )
#' 
#' #' Make a grid by aggregating up a list tree. 
#' make_grid = function(x){
#' 
#'   leaf_chk = vapply(x, function(l) is.atomic(l) || is.data.frame(l), T)
#'   
#'   # If so -- descend a level
#'   if(!all(leaf_chk)){
#'     x[!leaf_chk] = lapply(x[!leaf_chk], make_grid)
#'   }
#'   
#'   # 
#'   # 
#'   # if(is.list(x) && length(x) == 1) return(x)
#'   # if(is.atomic(x)) return(list(x))
#'   
#'   # When there are only leafs, construct the grid expander
#'   ge = lapply(x, function(z){
#'     if(is.data.frame(z)) return(seq_len(nrow(z)))
#'     z
#'   })
#'   browser()
#'   
#'   # Make the grid
#'   grd = expand.grid(ge, stringsAsFactors = FALSE)
#' 
#'   r = lapply(seq_len(ncol(grd)), function(n){
#'     if(is.data.frame(x[[n]])){
#'       return(x[[n]][grd[[n]],])
#'     }else{
#'       return(data.frame(grd[[n]]))
#'     }
#'   })
#'   do.call(cbind, r)
#' }
#' o = make_grid(x)
#' 
