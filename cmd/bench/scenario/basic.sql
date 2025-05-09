CREATE TABLE users (
	id NUMBER,
	name TEXT,
	email TEXT
);

CREATE TABLE posts (
	id NUMBER,
	title TEXT,
	content TEXT,
	user_id NUMBER
);

CREATE TABLE comments (
	id NUMBER,
	post_id NUMBER,
	user_id NUMBER,
	content TEXT
);

INSERT INTO users (id, name, email) VALUES
	(1, 'User 1', 'user1@example.com'), (2, 'User 2', 'user2@example.com'), (3, 'User 3', 'user3@example.com'), (4, 'User 4', 'user4@example.com'), (5, 'User 5', 'user5@example.com'),
	(6, 'User 6', 'user6@example.com'), (7, 'User 7', 'user7@example.com'), (8, 'User 8', 'user8@example.com'), (9, 'User 9', 'user9@example.com'), (10, 'User 10', 'user10@example.com'),
	(11, 'User 11', 'user11@example.com'), (12, 'User 12', 'user12@example.com'), (13, 'User 13', 'user13@example.com'), (14, 'User 14', 'user14@example.com'), (15, 'User 15', 'user15@example.com'),
	(16, 'User 16', 'user16@example.com'), (17, 'User 17', 'user17@example.com'), (18, 'User 18', 'user18@example.com'), (19, 'User 19', 'user19@example.com'), (20, 'User 20', 'user20@example.com'),
	(21, 'User 21', 'user21@example.com'), (22, 'User 22', 'user22@example.com'), (23, 'User 23', 'user23@example.com'), (24, 'User 24', 'user24@example.com'), (25, 'User 25', 'user25@example.com'),
	(26, 'User 26', 'user26@example.com'), (27, 'User 27', 'user27@example.com'), (28, 'User 28', 'user28@example.com'), (29, 'User 29', 'user29@example.com'), (30, 'User 30', 'user30@example.com'),
	(31, 'User 31', 'user31@example.com'), (32, 'User 32', 'user32@example.com'), (33, 'User 33', 'user33@example.com'), (34, 'User 34', 'user34@example.com'), (35, 'User 35', 'user35@example.com'),
	(36, 'User 36', 'user36@example.com'), (37, 'User 37', 'user37@example.com'), (38, 'User 38', 'user38@example.com'), (39, 'User 39', 'user39@example.com'), (40, 'User 40', 'user40@example.com'),
	(41, 'User 41', 'user41@example.com'), (42, 'User 42', 'user42@example.com'), (43, 'User 43', 'user43@example.com'), (44, 'User 44', 'user44@example.com'), (45, 'User 45', 'user45@example.com'),
	(46, 'User 46', 'user46@example.com'), (47, 'User 47', 'user47@example.com'), (48, 'User 48', 'user48@example.com'), (49, 'User 49', 'user49@example.com'), (50, 'User 50', 'user50@example.com'),
	(51, 'User 51', 'user51@example.com'), (52, 'User 52', 'user52@example.com'), (53, 'User 53', 'user53@example.com'), (54, 'User 54', 'user54@example.com'), (55, 'User 55', 'user55@example.com'),
	(56, 'User 56', 'user56@example.com'), (57, 'User 57', 'user57@example.com'), (58, 'User 58', 'user58@example.com'), (59, 'User 59', 'user59@example.com'), (60, 'User 60', 'user60@example.com'),
	(61, 'User 61', 'user61@example.com'), (62, 'User 62', 'user62@example.com'), (63, 'User 63', 'user63@example.com'), (64, 'User 64', 'user64@example.com'), (65, 'User 65', 'user65@example.com'),
	(66, 'User 66', 'user66@example.com'), (67, 'User 67', 'user67@example.com'), (68, 'User 68', 'user68@example.com'), (69, 'User 69', 'user69@example.com'), (70, 'User 70', 'user70@example.com'),
	(71, 'User 71', 'user71@example.com'), (72, 'User 72', 'user72@example.com'), (73, 'User 73', 'user73@example.com'), (74, 'User 74', 'user74@example.com'), (75, 'User 75', 'user75@example.com'),
	(76, 'User 76', 'user76@example.com'), (77, 'User 77', 'user77@example.com'), (78, 'User 78', 'user78@example.com'), (79, 'User 79', 'user79@example.com'), (80, 'User 80', 'user80@example.com'),
	(81, 'User 81', 'user81@example.com'), (82, 'User 82', 'user82@example.com'), (83, 'User 83', 'user83@example.com'), (84, 'User 84', 'user84@example.com'), (85, 'User 85', 'user85@example.com'),
	(86, 'User 86', 'user86@example.com'), (87, 'User 87', 'user87@example.com'), (88, 'User 88', 'user88@example.com'), (89, 'User 89', 'user89@example.com'), (90, 'User 90', 'user90@example.com'),
	(91, 'User 91', 'user91@example.com'), (92, 'User 92', 'user92@example.com'), (93, 'User 93', 'user93@example.com'), (94, 'User 94', 'user94@example.com'), (95, 'User 95', 'user95@example.com'),
	(96, 'User 96', 'user96@example.com'), (97, 'User 97', 'user97@example.com'), (98, 'User 98', 'user98@example.com'), (99, 'User 99', 'user99@example.com'), (100, 'User 100', 'user100@example.com');

INSERT INTO posts (id, title, content, user_id) VALUES
	(1, 'Post 1', 'Content 1', 1), (2, 'Post 2', 'Content 2', 2), (3, 'Post 3', 'Content 3', 3), (4, 'Post 4', 'Content 4', 4), (5, 'Post 5', 'Content 5', 5),
	(6, 'Post 6', 'Content 6', 6), (7, 'Post 7', 'Content 7', 7), (8, 'Post 8', 'Content 8', 8), (9, 'Post 9', 'Content 9', 9), (10, 'Post 10', 'Content 10', 10),
	(11, 'Post 11', 'Content 11', 11), (12, 'Post 12', 'Content 12', 12), (13, 'Post 13', 'Content 13', 13), (14, 'Post 14', 'Content 14', 14), (15, 'Post 15', 'Content 15', 15),
	(16, 'Post 16', 'Content 16', 16), (17, 'Post 17', 'Content 17', 17), (18, 'Post 18', 'Content 18', 18), (19, 'Post 19', 'Content 19', 19), (20, 'Post 20', 'Content 20', 20),
	(21, 'Post 21', 'Content 21', 21), (22, 'Post 22', 'Content 22', 22), (23, 'Post 23', 'Content 23', 23), (24, 'Post 24', 'Content 24', 24), (25, 'Post 25', 'Content 25', 25),
	(26, 'Post 26', 'Content 26', 26), (27, 'Post 27', 'Content 27', 27), (28, 'Post 28', 'Content 28', 28), (29, 'Post 29', 'Content 29', 29), (30, 'Post 30', 'Content 30', 30),
	(31, 'Post 31', 'Content 31', 31), (32, 'Post 32', 'Content 32', 32), (33, 'Post 33', 'Content 33', 33), (34, 'Post 34', 'Content 34', 34), (35, 'Post 35', 'Content 35', 35),
	(36, 'Post 36', 'Content 36', 36), (37, 'Post 37', 'Content 37', 37), (38, 'Post 38', 'Content 38', 38), (39, 'Post 39', 'Content 39', 39), (40, 'Post 40', 'Content 40', 40),
	(41, 'Post 41', 'Content 41', 41), (42, 'Post 42', 'Content 42', 42), (43, 'Post 43', 'Content 43', 43), (44, 'Post 44', 'Content 44', 44), (45, 'Post 45', 'Content 45', 45),
	(46, 'Post 46', 'Content 46', 46), (47, 'Post 47', 'Content 47', 47), (48, 'Post 48', 'Content 48', 48), (49, 'Post 49', 'Content 49', 49), (50, 'Post 50', 'Content 50', 50),
	(51, 'Post 51', 'Content 51', 51), (52, 'Post 52', 'Content 52', 52), (53, 'Post 53', 'Content 53', 53), (54, 'Post 54', 'Content 54', 54), (55, 'Post 55', 'Content 55', 55),
	(56, 'Post 56', 'Content 56', 56), (57, 'Post 57', 'Content 57', 57), (58, 'Post 58', 'Content 58', 58), (59, 'Post 59', 'Content 59', 59), (60, 'Post 60', 'Content 60', 60),
	(61, 'Post 61', 'Content 61', 61), (62, 'Post 62', 'Content 62', 62), (63, 'Post 63', 'Content 63', 63), (64, 'Post 64', 'Content 64', 64), (65, 'Post 65', 'Content 65', 65),
	(66, 'Post 66', 'Content 66', 66), (67, 'Post 67', 'Content 67', 67), (68, 'Post 68', 'Content 68', 68), (69, 'Post 69', 'Content 69', 69), (70, 'Post 70', 'Content 70', 70),
	(71, 'Post 71', 'Content 71', 71), (72, 'Post 72', 'Content 72', 72), (73, 'Post 73', 'Content 73', 73), (74, 'Post 74', 'Content 74', 74), (75, 'Post 75', 'Content 75', 75),
	(76, 'Post 76', 'Content 76', 76), (77, 'Post 77', 'Content 77', 77), (78, 'Post 78', 'Content 78', 78), (79, 'Post 79', 'Content 79', 79), (80, 'Post 80', 'Content 80', 80),
	(81, 'Post 81', 'Content 81', 81), (82, 'Post 82', 'Content 82', 82), (83, 'Post 83', 'Content 83', 83), (84, 'Post 84', 'Content 84', 84), (85, 'Post 85', 'Content 85', 85),
	(86, 'Post 86', 'Content 86', 86), (87, 'Post 87', 'Content 87', 87), (88, 'Post 88', 'Content 88', 88), (89, 'Post 89', 'Content 89', 89), (90, 'Post 90', 'Content 90', 90),
	(91, 'Post 91', 'Content 91', 91), (92, 'Post 92', 'Content 92', 92), (93, 'Post 93', 'Content 93', 93), (94, 'Post 94', 'Content 94', 94), (95, 'Post 95', 'Content 95', 95),
	(96, 'Post 96', 'Content 96', 96), (97, 'Post 97', 'Content 97', 97), (98, 'Post 98', 'Content 98', 98), (99, 'Post 99', 'Content 99', 99), (100, 'Post 100', 'Content 100', 100);

INSERT INTO comments (id, post_id, user_id, content) VALUES
	(1, 1, 1, 'Comment 1'), (2, 1, 2, 'Comment 2'), (3, 2, 3, 'Comment 3'), (4, 2, 4, 'Comment 4'), (5, 3, 5, 'Comment 5'),
	(6, 3, 6, 'Comment 6'), (7, 4, 7, 'Comment 7'), (8, 4, 8, 'Comment 8'), (9, 5, 9, 'Comment 9'), (10, 5, 10, 'Comment 10'),
	(11, 6, 11, 'Comment 11'), (12, 6, 12, 'Comment 12'), (13, 7, 13, 'Comment 13'), (14, 7, 14, 'Comment 14'), (15, 8, 15, 'Comment 15'),
	(16, 8, 16, 'Comment 16'), (17, 9, 17, 'Comment 17'), (18, 9, 18, 'Comment 18'), (19, 10, 19, 'Comment 19'), (20, 10, 20, 'Comment 20'),
	(21, 11, 21, 'Comment 21'), (22, 11, 22, 'Comment 22'), (23, 12, 23, 'Comment 23'), (24, 12, 24, 'Comment 24'), (25, 13, 25, 'Comment 25'),
	(26, 13, 26, 'Comment 26'), (27, 14, 27, 'Comment 27'), (28, 14, 28, 'Comment 28'), (29, 15, 29, 'Comment 29'), (30, 15, 30, 'Comment 30'),
	(31, 16, 31, 'Comment 31'), (32, 16, 32, 'Comment 32'), (33, 17, 33, 'Comment 33'), (34, 17, 34, 'Comment 34'), (35, 18, 35, 'Comment 35'),
	(36, 18, 36, 'Comment 36'), (37, 19, 37, 'Comment 37'), (38, 19, 38, 'Comment 38'), (39, 20, 39, 'Comment 39'), (40, 20, 40, 'Comment 40'),
	(41, 21, 41, 'Comment 41'), (42, 21, 42, 'Comment 42'), (43, 22, 43, 'Comment 43'), (44, 22, 44, 'Comment 44'), (45, 23, 45, 'Comment 45'),
	(46, 23, 46, 'Comment 46'), (47, 24, 47, 'Comment 47'), (48, 24, 48, 'Comment 48'), (49, 25, 49, 'Comment 49'), (50, 25, 50, 'Comment 50'),
	(51, 26, 51, 'Comment 51'), (52, 26, 52, 'Comment 52'), (53, 27, 53, 'Comment 53'), (54, 27, 54, 'Comment 54'), (55, 28, 55, 'Comment 55'),
	(56, 28, 56, 'Comment 56'), (57, 29, 57, 'Comment 57'), (58, 29, 58, 'Comment 58'), (59, 30, 59, 'Comment 59'), (60, 30, 60, 'Comment 60'),
	(61, 31, 61, 'Comment 61'), (62, 31, 62, 'Comment 62'), (63, 32, 63, 'Comment 63'), (64, 32, 64, 'Comment 64'), (65, 33, 65, 'Comment 65'),
	(66, 33, 66, 'Comment 66'), (67, 34, 67, 'Comment 67'), (68, 34, 68, 'Comment 68'), (69, 35, 69, 'Comment 69'), (70, 35, 70, 'Comment 70'),
	(71, 36, 71, 'Comment 71'), (72, 36, 72, 'Comment 72'), (73, 37, 73, 'Comment 73'), (74, 37, 74, 'Comment 74'), (75, 38, 75, 'Comment 75'),
	(76, 38, 76, 'Comment 76'), (77, 39, 77, 'Comment 77'), (78, 39, 78, 'Comment 78'), (79, 40, 79, 'Comment 79'), (80, 40, 80, 'Comment 80'),
	(81, 41, 81, 'Comment 81'), (82, 41, 82, 'Comment 82'), (83, 42, 83, 'Comment 83'), (84, 42, 84, 'Comment 84'), (85, 43, 85, 'Comment 85'),
	(86, 43, 86, 'Comment 86'), (87, 44, 87, 'Comment 87'), (88, 44, 88, 'Comment 88'), (89, 45, 89, 'Comment 89'), (90, 45, 90, 'Comment 90'),
	(91, 46, 91, 'Comment 91'), (92, 46, 92, 'Comment 92'), (93, 47, 93, 'Comment 93'), (94, 47, 94, 'Comment 94'), (95, 48, 95, 'Comment 95'),
	(96, 48, 96, 'Comment 96'), (97, 49, 97, 'Comment 97'), (98, 49, 98, 'Comment 98'), (99, 50, 99, 'Comment 99'), (100, 50, 100, 'Comment 100');

select * from users;
select * from users;
select * from users;
select * from users;
select * from users;
select * from users;
select * from users;
select * from users;
select * from users;
select * from users;
select * from users where id = 2;
select * from users where id = 2;
select * from users where id = 2;
select * from users where id = 2;
select * from users where id = 2;
select * from users where id = 2;
select * from users where id = 2;
select * from users where id = 2;
select * from users where id = 2;
select * from users where id = 2;
select * from users where name = 'User 3';
select * from users where name = 'User 3';
select * from users where name = 'User 3';
select * from users where name = 'User 3';
select * from users where name = 'User 3';
select * from users where name = 'User 3';
select * from users where name = 'User 3';
select * from users where name = 'User 3';
select * from users where name = 'User 3';
select * from users where name = 'User 3';
select users.name, posts.content, comments.content
from users
join posts on posts.user_id = users.id
join comments on comments.post_id = posts.id
where users.id = 17;
select users.name, posts.content, comments.content
from users
join posts on posts.user_id = users.id
join comments on comments.post_id = posts.id
where users.id = 17;
select users.name, posts.content, comments.content
from users
join posts on posts.user_id = users.id
join comments on comments.post_id = posts.id
where users.id = 17;
select users.name, posts.content, comments.content
from users
join posts on posts.user_id = users.id
join comments on comments.post_id = posts.id
where users.id = 17;
select users.name, posts.content, comments.content
from users
join posts on posts.user_id = users.id
join comments on comments.post_id = posts.id
where users.id = 17;
select users.name, posts.content, comments.content
from users
join posts on posts.user_id = users.id
join comments on comments.post_id = posts.id
where users.id = 17;
select users.name, posts.content, comments.content
from users
join posts on posts.user_id = users.id
join comments on comments.post_id = posts.id
where users.id = 17;
select users.name, posts.content, comments.content
from users
join posts on posts.user_id = users.id
join comments on comments.post_id = posts.id
where users.id = 17;
select users.name, posts.content, comments.content
from users
join posts on posts.user_id = users.id
join comments on comments.post_id = posts.id
where users.id = 17;
select users.name, posts.content, comments.content
from users
join posts on posts.user_id = users.id
join comments on comments.post_id = posts.id
where users.id = 17;
select users.name, posts.content, comments.content
from users
join posts on posts.user_id = users.id
join comments on comments.post_id = posts.id
where users.id = 17;
