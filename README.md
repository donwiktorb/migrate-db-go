simple old script we've used to migrate our database to new format (few millions of rows) go is very fast (in development and in general) for this after testing with few other 
we are creating too many coroutines tho, which was fine in our case but to keep in mind, also we are not batching user inserts but it's old, was made in few mins just for our own use case 
