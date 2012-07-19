#include <assert.h>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include "mp1.h"

#define TIME_INTERVAL 1      /* 1s */
#define MAX_ARR_SIZE 1000

/*
 * For causal ordering implementation
 */
int stamps[MAX_ARR_SIZE] = {0};                                    // Vector timestamp of this process
int stamps_size;
int process_index;                              // identity of this particular process
char * messg;
struct node {
	int source;
	int * received_stamps;
	int source_index;
	char *message;
	struct node * next;
} node;
struct node * queue_head = NULL;              // remove from head
struct node * queue_tail = NULL;              // insert from tail

/*
 * For reliable multicast implementation
 */
char *messages[MAX_ARR_SIZE] = {0};
int messages_size = 0;

/*
 * For failure detection implementation
 */
pthread_t failure_thread;
int fail_array[MAX_ARR_SIZE] = {0};			// 0 denotes active process, 1 denotes crashed process
time_t last_received_time[MAX_ARR_SIZE] = {0};
time_t sending_time;

/*
 * Causal ordering helper functions
 */
// ### char const to char ###
void push_back(int source, int * received_stamps, int source_index,
	char *message) {
	if (queue_tail==NULL) {
		queue_head = (struct node*) malloc (sizeof(node));
		queue_tail = queue_head;
	} else {
		queue_tail -> next = (struct node*) malloc (sizeof(node));
		queue_tail = queue_tail -> next;
	}
	queue_tail -> source = source;
	queue_tail -> received_stamps = received_stamps;
	queue_tail -> source_index = source_index;
	queue_tail -> message = message;
}

void pop(struct node * prev) {
	if (prev==NULL) {
		free(queue_head);
		queue_head = NULL;
		queue_tail = NULL;
	} else {
		struct node * curr = prev -> next;
		prev -> next = curr -> next;
		free(curr);
		curr = NULL;
		if (prev -> next == NULL) {
			queue_tail = prev;
		}
	}
}

int can_deliver_messg(int * received_stamps, int source_index) {
	int can_deliver = 1;
	int i;
	// For casuality & Piggyback
	if (received_stamps[source_index] != stamps[source_index]+1) {
		return 0;
	}
	for (i=0; i<mcast_num_members; i++) {
		if (i!=source_index && received_stamps[i] > stamps[i]) {
			can_deliver = 0;
			break;
		}
	}
	return can_deliver;
}

void check_queue_deliver() {
	struct node * curr = queue_head;
	struct node * prev = NULL;
	while (curr != NULL) {
		if (can_deliver_messg(curr->received_stamps, curr->source_index)>0) {
			stamps[curr->source_index]++;
			deliver(curr->source, curr->message);
			curr = curr -> next;
			pop(prev);
		} else {
			prev = curr;
			curr = curr->next;
		}
	}
}

/*
 * For reliable multicast implementation
 */
void messages_insert(char * message) {
	messages_size++;
	messages[messages_size-1] = message;
}

/*
 * Failure detection helper functions
 */
void handle_failure(int index) {
// ### comment-out fail messg ###
//	printf("Process %d failed!\n", mcast_members[index]);
}

void *failure_thread_main(void *discard) 
{
// ### change char* to [] ###	
   char arg_messg[] = "alive?";
   while(1)
   {
		sending_time = time(0);
		int i;
		for (i = 0; i < mcast_num_members; i++) {
			if (i==process_index || fail_array[i] > 0) {
				continue;
			}
			if (last_received_time[i]!=0 &&
				difftime(sending_time,last_received_time[i])>TIME_INTERVAL*5) {
				handle_failure(i);
				fail_array[i] = 1;
			} else {
				usend(mcast_members[i], arg_messg, strlen(arg_messg)+1);
			}
		}
		
		sleep(TIME_INTERVAL);
	}

}

void multicast_init(void) {
	unicast_init();

	process_index = mcast_num_members -1;
	stamps_size = mcast_num_members;
	int i;

    /* start failure detection thread */

	if (pthread_create(&failure_thread, NULL, &failure_thread_main, NULL) != 0) {
		fprintf(stderr, "Error in pthread_create\n");
		exit(1);
	}

}

/* Basic multicast implementation */
void multicast(const char *message) {

	stamps[process_index] += 1;

	char * stamps_messg = (char *) malloc (100*mcast_num_members * (sizeof (char)));
	stamps_messg[0] = '\0';
	
	int i;
	for (i=0; i<mcast_num_members; i++) {
		char buffer[100];
		buffer[0] = '\0';
		sprintf (buffer, "%d \0", stamps[i]);
		strcat(stamps_messg, buffer);
	}
	strcat(stamps_messg, "\n\0");

// ### change size to size+10 ###
	char * arg_messg = (char *) malloc((strlen(stamps_messg) +
		strlen(message)+10) * sizeof(char));
	strcpy(arg_messg, stamps_messg);
	strcat(arg_messg, message);
	
	messages[stamps[process_index]] = arg_messg;

	for (i = 0; i < mcast_num_members; i++) {
		if (fail_array[i]==0) {
			usend(mcast_members[i], arg_messg, strlen(arg_messg)+1);
		}
	}
	
}

void receive(int source, const char *message, int len) {
	char * message_copy = (char *) malloc (strlen(message) * sizeof(char));
	message_copy[0] = '\0';
	strcpy(message_copy, message);
	
	int i;
	assert(message[len-1] == 0);
	
	// U have received a failure detection check
	if(strcmp (message, "alive?")==0)
	{
// ### change char * to char[] ###
		char arg_messg[] = "imalive";
		//B-Multicast back to the failure checker
		usend(source, arg_messg, strlen(arg_messg)+1); 
		return;
	}

	int source_index = -1;
	for (i = 0; i<mcast_num_members; i++)
	{
		if (mcast_members[i]==source)
		{
	        source_index = i;
	        break;
		}
	}
	
	if(strcmp (message, "imalive")==0)
	{
		last_received_time[source_index] = time(0);
		return;
    }
    
    // U have received a NACK
	if (strncmp (message,"nack\n",5) == 0) {
		char * arg_messg = (char *) malloc (strlen(message) * sizeof(int));
		arg_messg[0] = '\0';
		strcpy(arg_messg, message+5);
		int * missed_stamps = (int *) malloc ((stamps[source_index]+10) * sizeof(int));
		int len = 0;
		char * pch = strtok (arg_messg," ");
		while (pch != NULL) {
			missed_stamps[len] = atoi(pch);
			len++;
			pch = strtok (NULL, " ");
		}
		for (i=0; i<len; i++) {
			// check reliable conditions
			char * missed_messg = (char *) malloc (strlen(messages[missed_stamps[i]]) * sizeof(char));
			strcpy(missed_messg, messages[missed_stamps[i]]);
			usend(source, missed_messg, strlen(missed_messg)+1);
		}
// ### add return statement ###
		return;
	}

	i = 0;
	while (message[i] != '\n') {
		i++;
	}
	char * stamps_messg = (char *) malloc((i+1) * sizeof (char));
	stamps_messg[0] = '\0';
	
	char * real_messg = (char *) malloc(len * sizeof (char));
	real_messg[0] = '\0';
	strncpy(stamps_messg, message, i);
	strcpy(real_messg, message+i+1);

	int * received_stamps = (int *) malloc (mcast_num_members * sizeof(int));
	i = 0;
	char * pch = strtok (stamps_messg," ");
	while (pch != NULL) {
		received_stamps[i] = atoi(pch);
		i++;
		pch = strtok (NULL, " ");
	}

	int can_deliver = can_deliver_messg(received_stamps, source_index);
  
	if (source_index==process_index || can_deliver>0) {
	
		deliver(source, real_messg);
		if (source_index!=process_index) {
			stamps[source_index]++;
		}
	} else {
// ### change messgae to real_messg ###
    	push_back(source, received_stamps, source_index, real_messg);
    	
    	int diff = received_stamps[source_index] - (stamps[source_index]+1);
    	if (diff > 0) {
    		// U need to send an ACK back for messages missed
			char *arg_messg= (char *) malloc ((100*mcast_num_members+10) * sizeof(char));
			arg_messg[0] = '\0';
			strcat(arg_messg, "nack\n");
			char buffer[100];
			for (i=0; i<diff;i++) {
				int missing_index = stamps[source_index]+1+i;
				int n=sprintf (buffer, "%d ", missing_index);
				strcat(arg_messg, buffer);
			}
			
			//B-Multicast back to the failure checker
			usend(source, arg_messg, strlen(arg_messg)+1);
		}
	}
	check_queue_deliver();
}

void mcast_join(int member) {
}
