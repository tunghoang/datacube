SALT = 'salt'
JWT_ALGORITHM = "HS256"
BASE_PREFIX = '/api'
PUBLIC_GET_ROUTES = [ BASE_PREFIX+p for p in [
    '/products',
    '/measurements',
    '/datasets',
    '/datasets/download',
    '/datasets/time_limits',
    '/describe'
]]
PUBLIC_POST_ROUTES = [ BASE_PREFIX + p for p in [
    '/users/login',
    '/users/register'
]]
