�{���X�ϥλ���:
(�аѷ� RGs_���c�� )

1.RGs_table_gen.sql : �ΨӥͲ��C�Ѫ�RGs table (input => weblog�Breclog)

2.daily_shell_script.sh : ��crontab���覡�m�J�C��Ƶ{��
                          �|�۰ʧ�Q�Ѫ��~�B��B�����JRGs_table_gen.sql������
			  �гy�X�Q�Ѫ�RGs table

3.generate_many_days.sh : �妸����shell�����ɡA�i�ۤv�s��n���J���X�Ѫ��~�B��B��
                          �q�`�Ω󱼸�Ʈɻݭn�ɸ�ƪ��ɭ�

-------------------------------------------------------------------------------------

########### ��ʤƲ���table ###########

step-1.�ק�generate_many_days.sh��  , �[�J�Q�n����table�����

step-2.�blinux command line ��J:

	source �ϥΪ��s�����{/gohappy/generate_many_days.sh

	��

	source �ϥΪ��s�����{/momo/generate_many_days.sh


step-3.press "enter"

########### �]�mcrontab�۰ʤ� ###########

step-1.�bdaily_shell_script.sh����DATE=$(date +"%Y%m%d" --date="1 days ago")���X�Q�Ѥ��
       �A�z�L������O���᭱�[�W -hiveconf �ǤJDATE�ܼ�
       ������O��file����${hiveconf:DATE}�����ܼ�

*�`�N:���n�[�W�L�ת��޸�'�Υ[��+ �]��linux���r��ۥ[�O���ݭn��L�B�⤸��,��b�@�_�N�n

step-2.��crontab�[�J�Ƶ{����k:
     (1)��crontab -e �s��Ƶ{
     (2)�A��i�i�Jvim���s��Ҧ�
     (3)�s�觹���esc
     (4)���ۿ�J:wq ��enter (���write & quit)

*�z�Lcrontab -l �i�H�ݥثe�����Ƶ{

step-3.�]�w�Ƶ{���榡
	ex:
        	20 15 * * * source home/W100.ITRI/u453041/daily_shell_script.sh 
		 ^  ^	^     ^       ^
	���:�b	�� ��  any   ����   script    
        	        ��
  			��
  	         (�ѡB�g�B��)


1.�Y�n����daily�۰ʤƪ��ʧ@,�����blinux�I�U����demo_shell_script.sh���{���X�Y�i

2.�Y�n���create���N�@�Ѫ�table,�h
    �Ndemo_shell_script.sh���{���X���� YEAR,MONTH,DATE �令�Q�n���~,��,��Y�i



