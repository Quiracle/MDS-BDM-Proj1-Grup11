db = db.getSiblingDB('protondb');

db.createUser({
  user: 'admin',
  pwd: 'password',
  roles: [
    { role: 'readWrite', db: 'protondb' },
    { role: 'dbAdmin', db: 'protondb' }
  ]
});

// Create collections
db.createCollection('games');
db.createCollection('reports');
db.createCollection('process_status'); 