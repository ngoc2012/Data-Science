# **************************************************************************** #
#                                                                              #
#                                                         :::      ::::::::    #
#    Makefile                                           :+:      :+:    :+:    #
#                                                     +:+ +:+         +:+      #
#    By: dcyprien <dcyprien@student.42.fr>          +#+  +:+       +#+         #
#                                                 +#+#+#+#+#+   +#+            #
#    Created: 2022/09/28 10:56:09 by minh-ngu          #+#    #+#              #
#    Updated: 2024/11/12 12:15:29 by ngoc             ###   ########.fr        #
#                                                                              #
# **************************************************************************** #

all: gitd

gitd:
	git add -A -- :!*.o :!*.swp :!*.env :!*.crt :!*.key
	git commit -m "$(M)"
	git push
	
.PHONY: all clean fclean re test
